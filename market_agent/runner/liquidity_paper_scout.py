"""
liquidity_paper_scout.py
═══════════════════════════════════════════════════════════════════
Paper-trading scout for the Liquidity-Sweep Brain (H1 Equity).

FULLY SELF-CONTAINED:
  - Imports liquidity_postgres_patch to extend PostgresStorage with
    the liquidity_paper_signals table and 3 new methods
  - Data: ScraperAPI (Tier 1) → Breeze for .NS (Tier 1 Indian) → plain yfinance (Tier 2)
  - Brain: liquidity_sweep_signal() — locked params from 270d backtest
  - Logger: brain_reasoning_logger — plain-English explanation per decision

WHAT HAPPENS EACH CYCLE (~55 min, once per H1 candle):
  1. RESOLVE  — check every open signal vs current price → T1_HIT/SL_HIT/EXPIRED
  2. SCAN     — fetch fresh H1 data, compute regime, run brain
  3. GATE     — skip HOLD, conf < 0.65, CHAOS/SQUEEZE regime
  4. LOG      — explain_signal() writes plain-English reasoning to log
  5. STORE    — write new signal to liquidity_paper_signals table
  6. REPORT   — print cumulative performance every 3 cycles

LOCKED PARAMETERS (from 270d grid: vol=1.5 dist=0.3 piv=2 age=5):
  270d evidence: n=17, WR=58.8%, EV=1.337R, MaxDD=2R

SYMBOLS (8 — US via ScraperAPI, India via Breeze → ScraperAPI → yfinance):
  US:    AAPL, AMD, GOOGL, NVDA
  India: LT.NS, TATASTEEL.NS, RELIANCE.NS, ITC.NS

HOW TO RUN LOCALLY:
  python liquidity_paper_scout.py

HOW TO DEPLOY ON RENDER:
  See render_liquidity.yaml — Background Worker.
  Set DATABASE_URL, SCRAPER_API_KEY, and (for India) BREEZE_* env vars.
"""
from __future__ import annotations

import os
import sys
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import structlog
import pandas as pd
import requests
import yfinance as yf

# Load .env file when running locally
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = structlog.get_logger()


# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

BRAIN_NAME        = "Liquidity-Sweep"
INTERVAL_MINUTES  = int(os.getenv("LIQUIDITY_SCOUT_INTERVAL_MIN", "55"))

# 8 symbols — US + India
# Indian symbols (.NS) need Breeze for reliable data on cloud IPs
SYMBOLS_US     = ["AAPL", "AMD", "GOOGL", "NVDA"]
SYMBOLS_INDIA  = ["LT.NS", "TATASTEEL.NS", "RELIANCE.NS", "ITC.NS"]
ALL_SYMBOLS    = SYMBOLS_US + SYMBOLS_INDIA

# Data fetch: 90 calendar days → guarantees 300+ H1 trading bars
DATA_PERIOD    = "90d"
DATA_INTERVAL  = "1h"

# Brain gates
MIN_CONFIDENCE  = 0.65    # must match brain's _MIN_CONFIDENCE_GATE
NO_TRADE_REGIMES = {"CHAOS", "SQUEEZE"}   # skip these regimes entirely

# Go-live decision thresholds (from 270d backtest baseline)
MIN_WR          = 0.55   # minimum WR to go live
MIN_EV          = 0.40   # minimum EV in R
MAX_DD_R        = 8.0    # maximum drawdown in R before pausing
MIN_RESOLVED    = 15     # minimum resolved trades before live decision

# Max H1 bars before a trade expires (mirrors backtester)
MAX_BARS_IN_TRADE = 20

# ScraperAPI daily call limit guard
SCRAPER_DAILY_LIMIT = int(os.getenv("SCRAPER_DAILY_LIMIT", "900"))
_scraper_calls      = 0
_scraper_reset_date = datetime.now(timezone.utc).date()


# ═══════════════════════════════════════════════════════════════
# MARKET HOURS — per asset class, correctly implemented
# ═══════════════════════════════════════════════════════════════

def _is_market_open(symbol: str) -> bool:
    """
    Returns True only when the relevant exchange is open.

    Indian equities (.NS): Mon-Fri 09:15-15:30 IST (03:45-10:00 UTC)
    US equities:           Mon-Fri 09:30-16:00 ET  (13:30-20:00 UTC)
    """
    now_utc = datetime.now(timezone.utc)

    if ".NS" in symbol or ".BO" in symbol:
        IST     = timezone(timedelta(hours=5, minutes=30))
        now_ist = now_utc.astimezone(IST)
        if now_ist.weekday() >= 5:
            return False
        t = now_ist.hour * 60 + now_ist.minute
        return (9 * 60 + 15) <= t <= (15 * 60 + 30)

    # US equities — using EDT (UTC-4) year-round for simplicity
    ET     = timezone(timedelta(hours=-4))
    now_et = now_utc.astimezone(ET)
    if now_et.weekday() >= 5:
        return False
    t = now_et.hour * 60 + now_et.minute
    return (9 * 60 + 30) <= t <= (16 * 60)


# ═══════════════════════════════════════════════════════════════
# DATA FETCHING — 3-tier waterfall
#
# Indian (.NS): Breeze (T1) → ScraperAPI (T2) → plain yfinance (T3)
# US:           ScraperAPI (T1) → plain yfinance (T2)
# ═══════════════════════════════════════════════════════════════

_cache: Dict[str, Dict] = {}
_CACHE_TTL_MIN = 50


def _scraper_quota_ok() -> bool:
    global _scraper_calls, _scraper_reset_date
    today = datetime.now(timezone.utc).date()
    if today != _scraper_reset_date:
        _scraper_calls = 0
        _scraper_reset_date = today
    return _scraper_calls < SCRAPER_DAILY_LIMIT


def _fetch_scraperapi(symbol: str) -> Optional[pd.DataFrame]:
    global _scraper_calls
    api_key = os.getenv("SCRAPER_API_KEY", "")
    if not api_key or not _scraper_quota_ok():
        return None

    _PERIOD_MAP = {"90d": "3mo", "60d": "3mo", "30d": "1mo"}
    yf_range   = _PERIOD_MAP.get(DATA_PERIOD, "3mo")
    proxy_url  = f"http://scraperapi:{api_key}@proxy-server.scraperapi.com:8001"
    url        = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"

    try:
        resp = requests.get(
            url,
            params={"interval": DATA_INTERVAL, "range": yf_range},
            proxies={"http": proxy_url, "https": proxy_url},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                                   "Chrome/122.0.0.0 Safari/537.36"},
            verify=False,
            timeout=20,
        )
        if resp.status_code != 200:
            return None

        data   = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            return None

        result     = result[0]
        timestamps = result.get("timestamp", [])
        ohlcv      = result.get("indicators", {}).get("quote", [{}])[0]

        if not timestamps or not ohlcv.get("close"):
            return None

        df = pd.DataFrame({
            "Open":   ohlcv.get("open",   [None] * len(timestamps)),
            "High":   ohlcv.get("high",   [None] * len(timestamps)),
            "Low":    ohlcv.get("low",    [None] * len(timestamps)),
            "Close":  ohlcv.get("close",  [None] * len(timestamps)),
            "Volume": ohlcv.get("volume", [0]    * len(timestamps)),
        }, index=pd.to_datetime(timestamps, unit="s"))
        df.dropna(inplace=True)

        if len(df) < 100:
            return None

        _scraper_calls += 1
        return df

    except Exception as e:
        logger.debug("scraper_failed", symbol=symbol, error=str(e)[:80])
        return None


def _fetch_breeze(symbol: str) -> Optional[pd.DataFrame]:
    """
    Breeze (ICICI Direct) for Indian NSE stocks only.
    Requires: BREEZE_API_KEY, BREEZE_SECRET, BREEZE_SESSION_TOKEN env vars.
    Token expires daily — refresh in Render dashboard each morning.
    """
    if ".NS" not in symbol and ".BO" not in symbol:
        return None

    api_key = os.getenv("BREEZE_API_KEY", "")
    secret  = os.getenv("BREEZE_SECRET",  "")
    token   = os.getenv("BREEZE_SESSION_TOKEN", "")

    if not api_key or not secret or not token:
        return None

    try:
        from breeze_connect import BreezeConnect

        # isec_stock_code mappings (confirmed via breeze.get_names())
        # Add new .NS symbols here as needed
        _CODE_MAP = {
            "LT":        "LARTOU",
            "TATASTEEL": "TATSTE",
            "RELIANCE":  "RELI",
            "ITC":       "ITC",
        }

        breeze     = BreezeConnect(api_key=api_key)
        breeze.generate_session(api_secret=secret, session_token=token)

        nse_ticker  = symbol.replace(".NS", "").replace(".BO", "")
        bare_symbol = _CODE_MAP.get(nse_ticker, nse_ticker)
        exchange    = "NSE" if ".NS" in symbol else "BSE"

        days    = 90
        from_dt = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00.000Z")
        to_dt   = datetime.now().strftime("%Y-%m-%dT23:59:59.000Z")

        resp = breeze.get_historical_data_v2(
            interval="30minute",
            from_date=from_dt,
            to_date=to_dt,
            stock_code=bare_symbol,
            exchange_code=exchange,
            product_type="Cash",
        )

        if not resp or resp.get("Status") != 200:
            err = resp.get("Error", "unknown") if resp else "no response"
            if "session" in str(err).lower() or "token" in str(err).lower():
                logger.warning("breeze_token_expired", symbol=symbol,
                               msg="Refresh BREEZE_SESSION_TOKEN in Render dashboard")
            return None

        rows = resp.get("Success", [])
        if not rows:
            return None

        df = pd.DataFrame(rows)
        df.index = pd.to_datetime(df["datetime"])
        df = df[["open", "high", "low", "close", "volume"]].copy()
        df.columns = ["Open", "High", "Low", "Close", "Volume"]
        df = df.apply(pd.to_numeric, errors="coerce")
        df.dropna(inplace=True)
        df.sort_index(inplace=True)

        # Resample 30min → 1h
        df = df.resample("1h").agg({
            "Open": "first", "High": "max",
            "Low": "min",    "Close": "last", "Volume": "sum",
        }).dropna()

        return df if len(df) >= 100 else None

    except ImportError:
        logger.warning("breeze_not_installed")
        return None
    except Exception as e:
        err = str(e)
        if "session" in err.lower() or "token" in err.lower():
            logger.warning("breeze_token_expired", symbol=symbol,
                           msg="Refresh BREEZE_SESSION_TOKEN in Render dashboard")
        else:
            logger.debug("breeze_failed", symbol=symbol, error=err[:80])
        return None


def _fetch_yfinance_plain(symbol: str) -> Optional[pd.DataFrame]:
    """Plain yfinance — fallback tier, works locally, may be blocked on Render cloud IPs."""
    try:
        df = yf.Ticker(symbol).history(period=DATA_PERIOD, interval=DATA_INTERVAL)
        if df is None or df.empty:
            return None
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.dropna(inplace=True)
        return df if len(df) >= 100 else None
    except Exception as e:
        logger.debug("yf_plain_failed", symbol=symbol, error=str(e)[:80])
        return None


def _fetch_ohlcv(symbol: str) -> Optional[pd.DataFrame]:
    """
    3-tier data waterfall. Result is cached for 50 min.
    Indian: Breeze (T1) → ScraperAPI (T2) → yfinance (T3)
    US:     ScraperAPI (T1) → yfinance (T2)
    """
    hit = _cache.get(symbol)
    if hit:
        age_min = (datetime.now() - hit["fetched_at"]).total_seconds() / 60
        if age_min < _CACHE_TTL_MIN:
            return hit["df"]

    def _store(df, tier):
        logger.debug("data_ok", symbol=symbol, tier=tier, bars=len(df))
        _cache[symbol] = {"df": df, "fetched_at": datetime.now()}
        return df

    is_indian = ".NS" in symbol or ".BO" in symbol

    if is_indian:
        df = _fetch_breeze(symbol)
        if df is not None:
            return _store(df, "T1-Breeze")
        df = _fetch_scraperapi(symbol)
        if df is not None:
            return _store(df, "T2-ScraperAPI")
        df = _fetch_yfinance_plain(symbol)
        if df is not None:
            return _store(df, "T3-yf-plain")
    else:
        df = _fetch_scraperapi(symbol)
        if df is not None:
            return _store(df, "T1-ScraperAPI")
        df = _fetch_yfinance_plain(symbol)
        if df is not None:
            return _store(df, "T2-yf-plain")

    logger.warning("data_all_tiers_failed", symbol=symbol)
    return None


def _get_current_price(symbol: str, fallback_df: pd.DataFrame) -> float:
    """Get current market price. Falls back to last bar close."""
    api_key = os.getenv("SCRAPER_API_KEY", "")
    if api_key and _scraper_quota_ok():
        proxy_url = f"http://scraperapi:{api_key}@proxy-server.scraperapi.com:8001"
        try:
            resp = requests.get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}",
                params={"interval": "1m", "range": "1d"},
                proxies={"http": proxy_url, "https": proxy_url},
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                       "AppleWebKit/537.36"},
                verify=False,
                timeout=10,
            )
            if resp.status_code == 200:
                meta  = resp.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
                price = meta.get("regularMarketPrice") or meta.get("previousClose")
                if price and float(price) > 0:
                    return float(price)
        except Exception:
            pass

    try:
        price = yf.Ticker(symbol).fast_info.last_price
        if price and float(price) > 0:
            return float(price)
    except Exception:
        pass

    return float(fallback_df["Close"].iloc[-1])


# ═══════════════════════════════════════════════════════════════
# SESSION PERFORMANCE TRACKER (in-memory)
# ═══════════════════════════════════════════════════════════════

class SessionTracker:
    def __init__(self):
        self.signals: List[Dict] = []

    def record(self, symbol, direction, entry, t1, sl, regime, conf, reason):
        sl_dist = abs(entry - sl)
        rr = round(abs(t1 - entry) / sl_dist, 2) if sl_dist > 0 else 0.0
        self.signals.append({
            "time": datetime.now().strftime("%H:%M"), "symbol": symbol,
            "direction": direction, "entry": entry, "t1": t1, "sl": sl,
            "rr": rr, "regime": regime, "conf": round(conf, 2),
            "reason": reason[:80] if reason else "",
        })

    def print_summary(self):
        n = len(self.signals)
        if n == 0:
            print("  Session signals: 0  (no signals fired yet)")
            return
        print(f"  Session signals: {n}  (last 5 shown)")
        for s in self.signals[-5:]:
            print(f"    {s['time']}  {s['symbol']:<15}  {s['direction']:<4}  "
                  f"entry={s['entry']:.4g}  T1={s['t1']:.4g}  SL={s['sl']:.4g}  "
                  f"RR={s['rr']:.1f}  regime={s['regime']:<14}  conf={s['conf']:.2f}")
            if s["reason"]:
                print(f"           reason: {s['reason']}")


# ═══════════════════════════════════════════════════════════════
# PERFORMANCE REPORT
# ═══════════════════════════════════════════════════════════════

def _print_performance(storage) -> None:
    p = storage.get_liquidity_performance(BRAIN_NAME)
    if not p:
        print("  [Performance] No resolved signals yet.")
        return

    total   = p["total"]
    decided = p["decided"]
    wins    = p["wins"]
    losses  = p["losses"]
    wr      = p["wr"]
    total_r = p["total_r"]
    ev      = p["ev"]
    max_dd  = p["max_dd"]
    open_n  = p["open"]

    wr_ok = wr     >= MIN_WR
    ev_ok = ev     >= MIN_EV
    dd_ok = max_dd <= MAX_DD_R

    print()
    print("  ╔═══════════════════════════════════════════════════════╗")
    print(f"  ║  PAPER TRADE PERFORMANCE — {BRAIN_NAME:<25}  ║")
    print("  ╠═══════════════════════════════════════════════════════╣")
    print(f"  ║  Total signals  : {total:>4}  ({open_n} still open)               ║")
    print(f"  ║  Decided (W+L)  : {decided:>4}  ({wins}W / {losses}L)                     ║")
    print(f"  ║  Win Rate       : {wr:>6.1%}   target ≥{MIN_WR:.0%}     {'✅' if wr_ok else '❌'}          ║")
    print(f"  ║  Avg EV (R)     : {ev:>+7.3f}  target >{MIN_EV:.1f}      {'✅' if ev_ok else '❌'}          ║")
    print(f"  ║  Total R        : {total_r:>+7.2f}R                              ║")
    print(f"  ║  Max Drawdown   : {max_dd:>6.2f}R   limit ≤{MAX_DD_R:.0f}R      {'✅' if dd_ok else '❌'}          ║")
    print("  ╠═══════════════════════════════════════════════════════╣")

    if decided < MIN_RESOLVED:
        remaining = MIN_RESOLVED - decided
        print(f"  ║  ⏳  Need {remaining:>2} more resolved signals before deciding.    ║")
        print(f"  ║     Continue paper trading.                           ║")
    elif wr_ok and ev_ok and dd_ok:
        print(f"  ║  🟢  READY FOR LIVE TRADING — all thresholds passed   ║")
        print(f"  ║     Go live at 0.5% risk/trade. Monitor daily.        ║")
    else:
        failing = []
        if not wr_ok: failing.append(f"WR={wr:.1%}")
        if not ev_ok: failing.append(f"EV={ev:+.3f}")
        if not dd_ok: failing.append(f"DD={max_dd:.2f}R")
        print(f"  ║  🔴  BELOW THRESHOLD: {', '.join(failing):<34}  ║")
        print(f"  ║     Continue paper trading — do NOT go live yet.      ║")

    print("  ╚═══════════════════════════════════════════════════════╝")

    recent = p.get("recent", [])
    if recent:
        print(f"\n  Last {len(recent)} resolved signals:")
        for r in recent:
            icon = "✅" if r["outcome"] == "T1_HIT" else ("❌" if r["outcome"] == "SL_HIT" else "⏸ ")
            pnl  = f"{r['pnl_r']:+.2f}R" if r["pnl_r"] is not None else "  N/A"
            ts   = r["created_at"].strftime("%m-%d %H:%M") if r["created_at"] else "?"
            bars = f"({r['bars_held']}b)" if r.get("bars_held") else ""
            print(f"    {icon}  {r['symbol']:<15}  {r['direction']:<4}  "
                  f"{r['outcome']:<8}  {pnl}  {bars:<6}  regime={r['regime'] or '?':<14}  {ts}")
    print()


# ═══════════════════════════════════════════════════════════════
# CORE SCAN — one complete cycle
# ═══════════════════════════════════════════════════════════════

UNIFIED_REGIMES = {"TRENDING_UP", "TRENDING_DOWN", "RANGING", "VOLATILE", "SQUEEZE", "CHAOS"}


def _run_scan_cycle(storage, tracker: SessionTracker,
                    brain_fn, regime_fn, reasoning_fn) -> int:
    """
    Scan all 8 symbols. Returns count of new signals stored this cycle.

    For each symbol:
      1. Market open check
      2. Fetch H1 OHLCV (90 days)
      3. Get current price
      4. Resolve open signals (T1/SL/EXPIRED)
      5. Compute regime
      6. Run brain → get BrainSignal
      7. Log plain-English reasoning
      8. Gate (HOLD, low conf, bad regime)
      9. Store signal in DB
    """
    new_signals = 0

    for symbol in ALL_SYMBOLS:

        # ── 1. Market open ──────────────────────────────────────
        if not _is_market_open(symbol):
            logger.debug("market_closed_skip", symbol=symbol)
            continue

        # ── 2. Fetch data ───────────────────────────────────────
        hist = _fetch_ohlcv(symbol)
        if hist is None or len(hist) < 200:
            logger.warning("insufficient_data", symbol=symbol,
                           bars=len(hist) if hist is not None else 0)
            continue

        # ── 3. Current price ────────────────────────────────────
        current_price = _get_current_price(symbol, hist)
        if current_price <= 0:
            logger.warning("bad_price", symbol=symbol, price=current_price)
            continue

        # ── 4. Resolve open signals BEFORE scanning ──────────────
        resolved = storage.resolve_liquidity_signals(symbol, current_price, MAX_BARS_IN_TRADE)
        if resolved:
            logger.info("signals_resolved", symbol=symbol, count=resolved)

        # ── 5. Compute regime ───────────────────────────────────
        regime = "RANGING"
        if regime_fn is not None:
            try:
                rs       = regime_fn(hist)
                computed = rs.measurements.get("computed_regime", "")
                if computed in UNIFIED_REGIMES:
                    regime = computed
            except Exception as re:
                logger.debug("regime_failed", symbol=symbol, error=str(re)[:60])

        # ── 6. Run brain ────────────────────────────────────────
        try:
            brain_signal = brain_fn(hist, symbol, regime)
        except Exception as be:
            logger.warning("brain_failed", symbol=symbol, error=str(be)[:80])
            continue

        # ── 7. Log plain-English reasoning ──────────────────────
        if reasoning_fn is not None:
            try:
                bar_time    = hist.index[-1]
                explanation = reasoning_fn(brain_signal, symbol, regime, bar_time)
                print(f"\n{'─'*60}")
                print(explanation)
            except Exception as le:
                logger.debug("reasoning_log_failed", symbol=symbol, error=str(le)[:60])

        # ── 8. Direction gate ───────────────────────────────────
        if brain_signal.direction in ("HOLD", "WAIT", None, ""):
            continue

        # ── 9. Confidence gate ──────────────────────────────────
        eff_conf = (
            brain_signal.effective_confidence()
            if hasattr(brain_signal, "effective_confidence")
            else brain_signal.confidence
        )
        if eff_conf < MIN_CONFIDENCE:
            logger.debug("low_conf_skip", symbol=symbol, conf=round(eff_conf, 3))
            continue

        # ── 10. Regime gate ─────────────────────────────────────
        if regime in NO_TRADE_REGIMES:
            logger.debug("regime_blocked", symbol=symbol, regime=regime)
            continue

        # ── 11. Build trade levels from brain measurements ───────
        m          = brain_signal.measurements or {}
        entry      = m.get("entry_price",  current_price)
        target_1   = m.get("target_1",     0.0)
        stop_loss  = m.get("stop_loss",    0.0)
        target_2   = target_1  # brain only computes T1; T2 = T1 for now

        # Sanity check levels
        sl_dist = abs(entry - stop_loss)
        if sl_dist <= 0 or target_1 <= 0 or stop_loss <= 0:
            logger.warning("bad_levels", symbol=symbol,
                           entry=entry, t1=target_1, sl=stop_loss)
            continue

        rr = round(abs(target_1 - entry) / sl_dist, 2)
        if rr < 1.5:
            logger.debug("rr_too_low", symbol=symbol, rr=rr)
            continue

        # ── 12. Store signal ────────────────────────────────────
        sig_id = storage.store_liquidity_signal(
            brain_name     = BRAIN_NAME,
            symbol         = symbol,
            direction      = brain_signal.direction,
            entry_price    = round(entry,     6),
            target_1       = round(target_1,  6),
            target_2       = round(target_2,  6),
            stop_loss      = round(stop_loss, 6),
            confidence     = round(eff_conf,  4),
            regime         = regime,
            timeframe      = DATA_INTERVAL,
            swept_level    = m.get("swept_level"),
            vol_ratio      = m.get("vol_ratio"),
            rsi_at_signal  = m.get("rsi"),
            touches        = m.get("touches"),
            wick_depth_atr = m.get("wick_depth_atr"),
            reason         = brain_signal.primary_evidence,
        )

        if sig_id:
            new_signals += 1
            tracker.record(symbol, brain_signal.direction, entry,
                           target_1, stop_loss, regime, eff_conf,
                           brain_signal.primary_evidence or "")

            print(f"\n  🟢 SIGNAL STORED [{sig_id}]")
            print(f"     {symbol:<15}  {brain_signal.direction:<4}  "
                  f"@ {entry:.4g}  T1={target_1:.4g}  SL={stop_loss:.4g}  "
                  f"RR={rr:.1f}  regime={regime}  conf={eff_conf:.2f}")
            print(f"     Evidence: {brain_signal.primary_evidence or 'N/A'}")

    return new_signals


# ═══════════════════════════════════════════════════════════════
# HEALTH SERVER — keeps Render Free Web Service alive
# Must bind port BEFORE Render's 60-second deadline
# ═══════════════════════════════════════════════════════════════

def _start_health_server():
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = b"liquidity-paper-scout: alive\n"
            self.send_response(200)
            self.send_header("Content-Type",   "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            pass  # silence per-request access logs

    port   = int(os.getenv("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"  ✅ Health server running on port {port}  (UptimeRobot: GET /health)")


# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def start_scout():
    """Entry point. Runs forever."""

    # ── Step 1: DB connection ─────────────────────────────────────
    try:
        print("  Importing liquidity_postgres_patch...")
        import market_agent.data.storage.liquidity_postgres_patch  # noqa
        print("  Importing PostgresStorage from postgres...")
        from market_agent.data.storage.postgres import PostgresStorage
        print("  Connecting to database...")
        storage = PostgresStorage()
        # Ensure liquidity_paper_signals table exists
        from market_agent.data.storage.liquidity_postgres_patch import (
            LiquidityPaperSignal, Base,
        )
        Base.metadata.create_all(storage.engine)
        print("  ✅ Database connected. liquidity_paper_signals table ready.")
    except Exception as e:
        import traceback
        print(f"  ❌ FATAL — Cannot connect to database: {e}")
        print("  Full traceback:")
        traceback.print_exc()
        print("     Ensure DATABASE_URL environment variable is set.")
        sys.exit(1)

    # ── Step 2: Load Liquidity-Sweep brain ────────────────────────
    try:
        from market_agent.brain.liquidity_sweep import liquidity_sweep_signal
        brain_fn = liquidity_sweep_signal
        print("  ✅ Brain loaded: Liquidity-Sweep v5 (vol=1.5 dist=0.3 piv=2 age=5)")
    except Exception as e:
        import traceback
        print(f"  ❌ FATAL — Cannot load brain: {e}")
        traceback.print_exc()
        sys.exit(1)

    # ── Step 3: Load regime brain (degrades gracefully) ───────────
    regime_fn = None
    try:
        from market_agent.brain.regime_ensemble import regime_ensemble_signal
        regime_fn = regime_ensemble_signal
        print("  ✅ Regime-Ensemble loaded.")
    except Exception as e:
        print(f"  ⚠️  Regime-Ensemble unavailable: {e}")
        print("     Defaulting to RANGING for all symbols.")

    # ── Step 4: Load reasoning logger (degrades gracefully) ───────
    reasoning_fn = None
    try:
        from market_agent.brain.brain_reasoning_logger import explain_signal
        reasoning_fn = explain_signal
        print("  ✅ Brain reasoning logger loaded.")
    except Exception as e:
        print(f"  ⚠️  Reasoning logger unavailable: {e}")
        print("     Signals will fire without plain-English explanation.")

    # ── Session tracker ───────────────────────────────────────────
    tracker = SessionTracker()

    # ── Startup banner ────────────────────────────────────────────
    print()
    print("═" * 65)
    print("  LIQUIDITY-SWEEP PAPER SCOUT  —  LIVE")
    print(f"  Brain    : Liquidity-Sweep v5 — Institutional Stop-Hunt Reversal")
    print(f"  Symbols  : {len(ALL_SYMBOLS)}  ({', '.join(SYMBOLS_US)} + {', '.join(SYMBOLS_INDIA)})")
    print(f"  Interval : {INTERVAL_MINUTES} min per cycle  (fires once per H1 candle)")
    print(f"  Timeframe: H1  |  Data window: 90 calendar days")
    print(f"  Locked params: vol≥1.5x  dist=0.3xATR  pivot=2  age≥5 bars")
    print(f"  Go-live gate : WR≥{MIN_WR:.0%}  EV>{MIN_EV:.1f}R  MaxDD≤{MAX_DD_R:.0f}R  n≥{MIN_RESOLVED}")
    print("═" * 65)
    print()
    print("  Market status at startup:")
    for s in ALL_SYMBOLS:
        status = "🟢 OPEN" if _is_market_open(s) else "🔴 closed"
        print(f"    {s:<22} {status}")
    print()

    # ── Main loop ─────────────────────────────────────────────────
    cycle = 0
    while True:
        cycle += 1
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{'─' * 65}")
        print(f"  CYCLE {cycle}  |  {ts}  |  brain={BRAIN_NAME}")
        print(f"{'─' * 65}")

        try:
            n = _run_scan_cycle(storage, tracker, brain_fn, regime_fn, reasoning_fn)

            if n == 0:
                print("  No new signals this cycle.")
            else:
                print(f"\n  {n} new signal(s) stored in liquidity_paper_signals.")

            print()
            tracker.print_summary()

            if cycle % 3 == 0:
                _print_performance(storage)

        except KeyboardInterrupt:
            print("\n\n  Stopped by user (Ctrl+C).")
            _print_performance(storage)
            sys.exit(0)

        except Exception as e:
            logger.error("cycle_error", cycle=cycle, error=str(e))
            print(f"  ⚠️  Unhandled error in cycle {cycle}: {e}")
            print("     Scout will continue on next cycle.")

        print(f"\n  ⏱  Sleeping {INTERVAL_MINUTES} min until next cycle...")
        time.sleep(INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    # Bind port IMMEDIATELY — before DB or brain loading
    # Render kills the process if no port is bound within 60 seconds
    _start_health_server()
    start_scout()