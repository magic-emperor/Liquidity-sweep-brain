"""
market_agent/data/storage/liquidity_postgres_patch.py
======================================================
DB table + 3 storage methods for the Liquidity-Sweep paper scout.

DESIGN PRINCIPLE:
  Does NOT modify postgres.py directly.
  Declares a new SQLAlchemy model (LiquidityPaperSignal) and patches
  three methods onto the existing PostgresStorage class at import time.
  Import this module once at the top of liquidity_paper_scout.py and
  PostgresStorage will gain the new methods automatically.

TABLE: liquidity_paper_signals
  Completely separate from paper_trade_signals (Causal-Ensemble data).
  Liquidity brain data never touches Causal data and vice versa.

KEY DIFFERENCES vs PaperTradeSignal (Causal):
  - Adds 'reason' column: plain-English HOLD reason from brain_reasoning_logger
  - Adds 'vol_ratio' column: volume confirmation value logged per signal
  - Adds 'rsi_at_signal' column: RSI value logged per signal
  - Adds 'swept_level' column: the structural level that was swept
  - Adds 'touches' column: how many times that level was touched before
  - Expiry is H1 BAR COUNT (20 bars = MAX_BARS_IN_TRADE), not wall-clock hours

METHODS PATCHED ONTO PostgresStorage:
  store_liquidity_signal(...)         → write new open signal
  resolve_liquidity_signals(...)      → check T1/SL/EXPIRY per symbol
  get_liquidity_performance(...)      → WR / EV / MaxDD performance report

USAGE:
  from market_agent.data.storage.postgres import PostgresStorage
  import market_agent.data.storage.liquidity_postgres_patch  # noqa — side effect
  storage = PostgresStorage()
  storage.store_liquidity_signal(...)  # now available
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column, DateTime, Float, Index, Integer, String,
)
from sqlalchemy.orm import declarative_base

log = logging.getLogger("liquidity_postgres_patch")


# ═══════════════════════════════════════════════════════════════
# SQLAlchemy model
# ═══════════════════════════════════════════════════════════════

# We need the same Base that postgres.py uses so the table is
# created via the same metadata. Import it from postgres.
try:
    from market_agent.data.storage.postgres import Base
except ImportError:
    # Fallback if running standalone tests
    Base = declarative_base()


class LiquidityPaperSignal(Base):
    """
    One row per paper-trade signal fired by the Liquidity-Sweep brain.

    outcome values:
      NULL     = still open (price has not yet hit T1, SL, or expired)
      T1_HIT   = target 1 was reached → WIN
      SL_HIT   = stop loss was hit     → LOSS
      EXPIRED  = 20 H1 bars elapsed without resolution
    """
    __tablename__ = "liquidity_paper_signals"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    brain_name      = Column(String,  nullable=False, index=True)   # "Liquidity-Sweep"
    symbol          = Column(String,  nullable=False, index=True)
    direction       = Column(String,  nullable=False)               # BUY | SELL
    entry_price     = Column(Float,   nullable=False)
    target_1        = Column(Float,   nullable=False)
    target_2        = Column(Float,   nullable=True)
    stop_loss       = Column(Float,   nullable=False)
    confidence      = Column(Float,   nullable=True)
    regime          = Column(String,  nullable=True)
    timeframe       = Column(String,  nullable=True, default="1h")

    # Liquidity-sweep-specific columns
    swept_level     = Column(Float,   nullable=True)    # structural level that was swept
    vol_ratio       = Column(Float,   nullable=True)    # volume spike ratio at signal
    rsi_at_signal   = Column(Float,   nullable=True)    # RSI value at signal bar
    touches         = Column(Integer, nullable=True)    # how many times level was touched
    wick_depth_atr  = Column(Float,   nullable=True)    # wick depth in ATR units
    reason          = Column(String,  nullable=True)    # plain-English signal reason

    # Resolution
    outcome         = Column(String,  nullable=True)    # NULL|T1_HIT|SL_HIT|EXPIRED
    exit_price      = Column(Float,   nullable=True)
    pnl_r           = Column(Float,   nullable=True)    # profit/loss in R units
    bars_held       = Column(Integer, nullable=True)    # H1 bars the trade was open
    created_at      = Column(DateTime, default=datetime.utcnow, index=True)
    resolved_at     = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_lps_brain_open",   "brain_name", "outcome"),
        Index("idx_lps_symbol_open",  "symbol",     "outcome"),
    )


# ═══════════════════════════════════════════════════════════════
# Methods to patch onto PostgresStorage
# ═══════════════════════════════════════════════════════════════

def _store_liquidity_signal(
    self,
    brain_name:    str,
    symbol:        str,
    direction:     str,
    entry_price:   float,
    target_1:      float,
    stop_loss:     float,
    confidence:    float,
    regime:        str,
    target_2:      float = None,
    swept_level:   float = None,
    vol_ratio:     float = None,
    rsi_at_signal: float = None,
    touches:       int   = None,
    wick_depth_atr:float = None,
    reason:        str   = None,
    timeframe:     str   = "1h",
) -> Optional[int]:
    """
    Store a new open Liquidity-Sweep paper-trade signal.
    Returns the row ID or None on failure.
    """
    session = self.Session()
    try:
        row = LiquidityPaperSignal(
            brain_name     = brain_name,
            symbol         = symbol,
            direction      = direction,
            entry_price    = entry_price,
            target_1       = target_1,
            target_2       = target_2,
            stop_loss      = stop_loss,
            confidence     = confidence,
            regime         = regime,
            timeframe      = timeframe,
            swept_level    = swept_level,
            vol_ratio      = vol_ratio,
            rsi_at_signal  = rsi_at_signal,
            touches        = touches,
            wick_depth_atr = wick_depth_atr,
            reason         = reason,
            outcome        = None,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return row.id
    except Exception as e:
        session.rollback()
        log.error("store_liquidity_signal_failed", extra={"error": str(e)[:120]})
        return None
    finally:
        session.close()


def _resolve_liquidity_signals(
    self,
    symbol:        str,
    current_price: float,
    max_bars:      int = 20,
) -> int:
    """
    Check all open Liquidity-Sweep signals for *symbol* against current_price.

    Resolution logic (mirrors backtester._evaluate_signal):
      BUY:  T1_HIT if current_price >= target_1
            SL_HIT if current_price <= stop_loss
      SELL: T1_HIT if current_price <= target_1
            SL_HIT if current_price >= stop_loss
      EXPIRED if bars_held >= max_bars (20 H1 bars = ~1 trading day on equities)

    NOTE: bars_held is incremented each call. Caller should call this once per
    H1 candle close (every ~55 min via the scout loop) per symbol.

    Returns count of signals resolved this call.
    """
    session = self.Session()
    resolved = 0
    try:
        open_sigs = session.query(LiquidityPaperSignal).filter(
            LiquidityPaperSignal.symbol  == symbol,
            LiquidityPaperSignal.outcome == None,   # noqa: E711
        ).all()

        now = datetime.utcnow()
        for sig in open_sigs:
            # Increment bar counter every call
            sig.bars_held = (sig.bars_held or 0) + 1

            sl_dist = abs(sig.entry_price - sig.stop_loss)
            if sl_dist <= 0:
                continue

            outcome    = None
            exit_price = current_price
            pnl_r      = None

            if sig.direction == "BUY":
                if current_price >= sig.target_1:
                    outcome = "T1_HIT"
                    pnl_r   = round((sig.target_1 - sig.entry_price) / sl_dist, 3)
                elif current_price <= sig.stop_loss:
                    outcome = "SL_HIT"
                    pnl_r   = -1.0
            else:  # SELL
                if current_price <= sig.target_1:
                    outcome = "T1_HIT"
                    pnl_r   = round((sig.entry_price - sig.target_1) / sl_dist, 3)
                elif current_price >= sig.stop_loss:
                    outcome = "SL_HIT"
                    pnl_r   = -1.0

            # Bar-count expiry (not time-based — consistent with backtester)
            if outcome is None and sig.bars_held >= max_bars:
                outcome = "EXPIRED"
                pnl_r   = round(
                    (current_price - sig.entry_price) / sl_dist
                    * (1 if sig.direction == "BUY" else -1),
                    3,
                )

            if outcome:
                sig.outcome     = outcome
                sig.exit_price  = exit_price
                sig.pnl_r       = pnl_r
                sig.resolved_at = now
                resolved += 1

        if resolved:
            session.commit()
        else:
            # Commit bar_held increments even if nothing resolved
            session.commit()

    except Exception as e:
        session.rollback()
        log.error("resolve_liquidity_signals_failed",
                  extra={"symbol": symbol, "error": str(e)[:120]})
    finally:
        session.close()
    return resolved


def _get_liquidity_performance(self, brain_name: str = "Liquidity-Sweep") -> Optional[dict]:
    """
    Compute cumulative paper-trade performance for the Liquidity-Sweep brain.

    Returns dict with: total, open, decided, wins, losses, wr, ev,
                       total_r, max_dd, recent (last 10 resolved)
    Returns None if no signals exist yet.
    """
    session = self.Session()
    try:
        all_sigs = (
            session.query(LiquidityPaperSignal)
            .filter(LiquidityPaperSignal.brain_name == brain_name)
            .order_by(LiquidityPaperSignal.created_at)
            .all()
        )
        if not all_sigs:
            return None

        total   = len(all_sigs)
        open_n  = sum(1 for s in all_sigs if s.outcome is None)
        decided = [s for s in all_sigs if s.outcome in ("T1_HIT", "SL_HIT")]
        wins    = [s for s in decided  if s.outcome == "T1_HIT"]
        losses  = [s for s in decided  if s.outcome == "SL_HIT"]

        n    = len(decided)
        wr   = len(wins) / n if n > 0 else 0.0
        pnls = [s.pnl_r for s in decided if s.pnl_r is not None]
        total_r = round(sum(pnls), 3) if pnls else 0.0
        ev      = round(total_r / n,  3) if n > 0 else 0.0

        # Peak-to-trough max drawdown in R
        max_dd  = 0.0
        peak    = 0.0
        running = 0.0
        for p in pnls:
            running += p
            if running > peak:
                peak = running
            dd = peak - running
            if dd > max_dd:
                max_dd = dd

        resolved_all = [s for s in all_sigs if s.outcome is not None]
        recent = [
            {
                "symbol":     s.symbol,
                "direction":  s.direction,
                "outcome":    s.outcome,
                "pnl_r":      s.pnl_r,
                "regime":     s.regime,
                "bars_held":  s.bars_held,
                "created_at": s.created_at,
            }
            for s in resolved_all[-10:]
        ]

        return {
            "total":   total,
            "open":    open_n,
            "decided": n,
            "wins":    len(wins),
            "losses":  len(losses),
            "wr":      round(wr, 4),
            "total_r": total_r,
            "ev":      ev,
            "max_dd":  round(max_dd, 3),
            "recent":  recent,
        }
    except Exception as e:
        log.error("get_liquidity_perf_failed",
                  extra={"brain": brain_name, "error": str(e)[:120]})
        return None
    finally:
        session.close()


# ═══════════════════════════════════════════════════════════════
# Patch onto PostgresStorage at import time
# ═══════════════════════════════════════════════════════════════

try:
    from market_agent.data.storage.postgres import PostgresStorage

    # Attach the 3 liquidity methods onto PostgresStorage.
    # No tometadata() call needed — LiquidityPaperSignal uses the same Base
    # as postgres.py so Base.metadata.create_all() picks it up automatically.
    # tometadata() was removed in SQLAlchemy 2.0 — do NOT use it.
    PostgresStorage.store_liquidity_signal      = _store_liquidity_signal
    PostgresStorage.resolve_liquidity_signals   = _resolve_liquidity_signals
    PostgresStorage.get_liquidity_performance   = _get_liquidity_performance

    log.info("liquidity_postgres_patch: methods patched onto PostgresStorage ✓")

except ImportError as _e:
    log.warning(f"liquidity_postgres_patch: PostgresStorage not found — "
                f"methods not patched ({_e}). Running in standalone mode.")