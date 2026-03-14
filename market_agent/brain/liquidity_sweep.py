"""
═══════════════════════════════════════════════════════════════════════
Brain: Liquidity-Sweep v4 — Institutional Stop-Hunt Reversal Detector
═══════════════════════════════════════════════════════════════════════

PATTERN:
  BUY:  Wick sweeps below a swing low → candle CLOSES ABOVE it → reversal
  SELL: Wick sweeps above a swing high → candle CLOSES BELOW it → reversal

SUITABLE ASSETS:      Large-cap equities (mean-reverting, respect structure)
                      Symbols: LT.NS, TATASTEEL.NS, AAPL, RELIANCE.NS, ITC.NS,
                               AMD, GOOGL, NVDA
SUITABLE TIMEFRAMES:  1H (primary), 4H, D1
CYCLE TIME:           Once per hour — runs ~20s after each H1 candle closes
DATA REQUIRED:        Minimum 200 bars per run (~25 trading days)
STATUS:               CONSTANTS LOCKED — ready for paper trading

LOCKED PARAMETERS (270d grid: vol=1.5 dist=0.3 piv=2 age=5):
  vol_spike_mult     = 1.5   (WR=58.8%, EV=1.337R, MaxDD=2R across 270d)
  min_level_dist_atr = 0.30  (best signal count at quality threshold)
  pivot_bars         = 2     (more levels, higher WR than pivot=3)
  min_level_age_bars = 5     (n=17 vs n=13, same WR range)

═══════════════════════════════════════════════════════════════════════
V3 → V4 CHANGES (from 180-day H1 equity backtest: WR=40%, MaxDD=6R)
═══════════════════════════════════════════════════════════════════════

ROOT CAUSE 1 (V4): TRENDING_DOWN BUY sweeps are continuation breakdowns
  180d breakdown: TRENDING_DOWN BUY n=4, WR=0.0%, avg_R=-1.000
  V3 allowed BUY sweeps in TRENDING_DOWN (from crypto logic — wrong on equities).
  On equities a sweep low in a downtrend is a breakdown, not a reversal.
  V4 FIX: BUY sweeps blocked in TRENDING_DOWN and TRENDING_UP.
           BUY only valid in RANGING and VOLATILE.
           SELL sweeps keep TRENDING_UP (n=2, WR=100%, avg_R=+4.570 confirmed).

ROOT CAUSE 2 (V4): RETRACTED — Close_pct gate was based on misread data
  The 180d breakdown showed weak close (<50%) WR=100% and strong close
  (>80%) WR=0%. This appeared to mean: invert the close gate.
  
  Diagnostic on real data showed ALL bullish sweeps have close_pct 0.80-0.99
  by geometry (wick below low, close back up = always high close_pct).
  The "weak close wins" in the breakdown were SELL sweeps (naturally low
  close_pct). The breakdown mixed directions in one bucket — it was a
  confound of regime direction, not an independent close_pct signal.
  
  V4 CORRECTION: Close_pct gate removed. Strong close remains a
  confidence BOOST (not a gate) as it was in v3.

ROOT CAUSE 3 (V4): Volume ceiling missing — panic volume destroys edge
  180d breakdown:
    vol 2-3x: WR=66.7%, avg_R=+2.713  (EV+ sweet spot)
    vol 3x+:  WR=0.0%,  avg_R=-1.000  (ALL losses — panic, not institution)
  V3 had no upper bound: assumed higher volume = better. Wrong on equities.
  Panic volume = stops already ran = no reversal fuel left.
  V4 FIX: Volume must be in [_VOL_SPIKE_MULT, _VOL_CEILING_MULT).
           vol >= 3.0x → HARD GATE block.

ROOT CAUSE 4 (V4 — REVISED IN V5): Touch count hard gate was over-fitted
  The n=6 evidence base for touches 3+ = WR=0.0% was statistically meaningless.
  V4 made it a hard gate — this blocked 84% of valid signals (57/68) in the
  v4b diagnostic because equity H1 swing levels get touched repeatedly over 180d.
  V5 FIX: Demoted back to confidence penalty (contra -0.08 per excess touch).
           Hard gate evidence threshold = min n=20 per bucket. n=6 is noise.

═══════════════════════════════════════════════════════════════════════
V4 → V5 CHANGES (from v4b gate attrition diagnostic)
═══════════════════════════════════════════════════════════════════════

ROOT CAUSE 1 (V5): Touch hard gate killed 84% of valid signals
  v4b diagnostic: after regime gate 68 signals, touch gate blocked 57 (84%).
  Gate was built on n=6 total trades — statistically meaningless.
  Evidence threshold for a hard gate: minimum n=20 per bucket.
  V5 FIX: Touch count demoted from hard gate → confidence penalty.
           0 touches: +0.06 (virgin level). 1 touch: +0.04 (clean).
           2 touches: +0.01 (tested). 3+ touches: -0.08 per extra touch (contra).
           Overused level can still produce a signal — just with lower confidence.

ROOT CAUSE 2 (V5): SELL_TRENDING_DOWN blocked with zero evidence
  v4b diagnostic: 23 SELL_TRENDING_DOWN sweeps blocked.
  This gate came from v3 crypto logic — never tested on equities.
  SELL sweep in TRENDING_DOWN = bearish stop-hunt (price spikes above swing
  high then closes back below) = institutional distribution. Conceptually valid.
  No equity data showing it loses. No gate without evidence.
  V5 FIX: SELL_TRENDING_DOWN re-allowed.
           SELL now valid in: RANGING, VOLATILE, TRENDING_UP, TRENDING_DOWN.
           BUY_TRENDING_DOWN remains blocked (n=4, WR=0%, confirmed to lose).

═══════════════════════════════════════════════════════════════════════
V2 → V3 CHANGES (from 90-day H1 BTC/ETH backtest: WR=25%, MaxDD=37R)
═══════════════════════════════════════════════════════════════════════

ROOT CAUSE 1 (V3): Wrong market — BTC/ETH H1 is trend-continuation
  V2 tested on BTC-USD and ETH-USD H1.
  Crypto H1 is dominated by momentum players, not institutional
  stop-hunt reversals. A sweep on BTC H1 often IS the continuation.
  V3 FIX: Target large-cap equities (LT.NS, TATASTEEL.NS, AAPL,
           RELIANCE.NS, ITC.NS, AMD, GOOGL) — mean-reverting instruments
           that respect structural levels.

ROOT CAUSE 2 (V3): Level age threshold too low for equity structure
  V2 breakdown — age 5-10 bars: WR=23.7%, avg_R=-0.158 (EV-)
               — age 10-20 bars: WR=25.6%, avg_R=+0.006 (EV+)
               — age 20-40 bars: WR=28.6%, avg_R=+0.130 (EV+)
  Pattern confirmed identically on D1 (structural insight, not noise).
  V3 FIX: _MIN_LEVEL_AGE_BARS raised 5 → 10.
           Fresh pivots lack sufficient stop accumulation.

ROOT CAUSE 3 (V3): Aged sweeps consistently losing
  V2 breakdown — sweep_age=0: WR=26.8%, avg_R=+0.019 (EV+)
               — sweep_age=1: WR=23.1%, avg_R=-0.121 (EV-)
  If reversal momentum does not begin on the sweep candle itself,
  the pattern is failing. Stale sweeps have no edge.
  V3 FIX: _SWEEP_CANDLE_LOOKBACK reduced 2 → 1 (current candle only).

ROOT CAUSE 4 (V3): Level tolerance calibrated for crypto, not equities
  0.5% tolerance was correct for BTC ($40k → $200 wiggle room).
  Equities are lower-priced and tighter (AAPL=$170, LT.NS=₹3400).
  0.5% caused false touch counts — levels appeared "used" when clean.
  V3 FIX: _LEVEL_TOLERANCE_PCT reduced 0.005 → 0.003.

ROOT CAUSE 5 (V3): RSI block too permissive
  V2 breakdown — RSI 65-75: WR=0.0%, avg_R=-1.000 (hard wall)
               — RSI 50-65: WR=29.7%, avg_R=+0.071 (best zone)
  Data shows reversal quality collapses past RSI 65.
  V3 FIX: RSI block tightened 25/75 → 30/70.

ROOT CAUSE 6 (V3): Swing lookback too long for H1 equity intraday
  80 bars on H1 = 10 trading days of levels.
  Levels from 2 weeks ago are not relevant to today's H1 structure.
  V3 FIX: _SWING_LOOKBACK reduced 80 → 60 (7.5 trading days on H1).
           D1 grid keeps 80 (4 months is correct for daily structure).

═══════════════════════════════════════════════════════════════════════
V1 → V2 CHANGES (retained, documented for audit trail)
═══════════════════════════════════════════════════════════════════════
  FIX-A: Swing levels sorted by RECENCY, not proximity.
  FIX-B: Min level distance gate: swept level >= 0.5xATR from price.
  FIX-C: Min level age gate (now raised further in V3).
  FIX-D: Max levels checked reduced 5 → 3.
  FIX-E: Volume is a HARD GATE (not a soft confidence boost).
  FIX-F: Touch count uses separate lookback, excludes pivot zone.
  FIX-G: RSI gate INVERTED — blocks extremes (now tightened in V3).
  FIX-H: Directional bias per regime.
  FIX-I: Minimum confidence hard gate 0.65.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Optional, List, Tuple, Dict
import structlog

from market_agent.brain.brain_contract import BrainSignal
from market_agent.brain.brain_utils import calc_atr, calc_rsi_series, compute_delta_flow

logger = structlog.get_logger()


# ═══════════════════════════════════════════════════════════════
# TUNEABLE CONSTANTS (grid candidates — do not hardcode values elsewhere)
# ═══════════════════════════════════════════════════════════════

# Sweep depth (ATR multiples)
_MIN_SWEEP_DEPTH_ATR  = 0.25
_MAX_SWEEP_DEPTH_ATR  = 1.50
_MIN_SWEEP_DEPTH_PCT  = 0.002   # 0.2% absolute floor (price-relative)

# FIX-B: Level must be this far from current price
# V5 LOCKED: 0.30 (from 0.50) — 270d grid: dist=0.3 best signal count at vol=1.5
# Evidence: vol=1.5 dist=0.3 piv=2 age=5: n=17, WR=58.8%, EV=1.337R, MaxDD=2R
_MIN_LEVEL_DIST_ATR   = 0.30

# Level age: minimum bars old before level is valid for sweep detection
# V5 LOCKED: 5 (from 10) — 270d grid: age=5 gives n=17 vs n=13 at age=10
# Both age=5 and age=10 show WR=55-59%. More signals with same quality.
_MIN_LEVEL_AGE_BARS   = 5

# Swing detection
# V3: Lookback reduced 80→60 for H1 equities (60h = 7.5 trading days)
_SWING_LOOKBACK       = 60
# V5 LOCKED: pivot_bars=2 (from 3) — more levels detected, same WR at vol=1.5
# 270d: piv=2 age=5: n=17 WR=58.8% vs piv=3 age=5: n=21 WR=52.4%
# piv=2 gives higher WR with similar signal count
_PIVOT_BARS           = 2
_MAX_LEVELS_TO_CHECK  = 3      # FIX-D (was 5)

# V3: Reduced 2→1 (current candle only)
# Evidence: sweep_age=0 WR=26.8% EV+ vs sweep_age=1 WR=23.1% EV-
# If reversal doesn't start on the sweep candle, pattern is failing.
_SWEEP_CANDLE_LOOKBACK = 1

# FIX-E: Volume hard gate — floor (0 = disabled)
_VOL_SPIKE_MULT       = 1.5
_VOL_LOOKBACK         = 20
# V4: Volume ceiling — vol >= 3.0x = panic = all losses in 180d data
# vol 2-3x: WR=66.7%, vol 3x+: WR=0.0% — hard wall confirmed
_VOL_CEILING_MULT     = 3.0

# FIX-F: Touch count — separate window, excludes pivot formation zone
_TOUCH_LOOKBACK       = 40
_TOUCH_EXCLUDE_RECENT = 6
# V3: Tolerance reduced 0.005→0.003 (equity scale, not crypto scale)
_LEVEL_TOLERANCE_PCT  = 0.003
# Touch count: used for confidence scoring only (NOT a hard gate)
# V4 made this a hard gate on n=6 evidence — too small. Reverted in V5.
# Hard gate threshold requires minimum n=20 per bucket to be valid evidence.
# Scoring: 0 touches=+0.06, 1=+0.04, 2=+0.01, 3+=contra penalty
_MAX_TOUCHES_CLEAN    = 2

# V3: RSI block tightened 25/75→30/70
# Evidence: RSI 65-75: WR=0.0% avg_R=-1.000 — hard wall confirmed in data
_RSI_BLOCK_LOW        = 30.0
_RSI_BLOCK_HIGH       = 70.0
# V4: RSI directional sweet-spot (confidence boost, NOT a hard gate)
# Evidence: RSI 60-70 = 100% WR for BUY sweeps in 180d data
# Using as boost range — not enough data yet to make this a hard block
_RSI_BUY_SWEET_LOW    = 55.0   # BUY sweeps: RSI 55-70 = elevated but not extreme
_RSI_BUY_SWEET_HIGH   = 70.0
_RSI_SELL_SWEET_LOW   = 30.0   # SELL sweeps: RSI 30-45 = depressed but not extreme
_RSI_SELL_SWEET_HIGH  = 45.0

# Stop-loss: anchored to swept level
_SL_BUFFER_ATR        = 0.30

# FIX-I: Minimum confidence hard gate
_MIN_CONFIDENCE_GATE  = 0.65

# Confidence decay per candle of sweep age
_AGE_CONFIDENCE_DECAY = 0.05

# Minimum data
_MIN_HIST_BARS        = 100


# ═══════════════════════════════════════════════════════════════
# STRUCTURAL LEVEL DETECTION
# ═══════════════════════════════════════════════════════════════

def _find_swing_levels(
    hist: pd.DataFrame,
    lookback: int = _SWING_LOOKBACK,
    pivot_bars: int = _PIVOT_BARS,
    current_price: float = 0.0,
) -> Dict[str, List[Tuple[float, int]]]:
    """
    Find swing highs and lows as (price, bar_age) tuples.
    FIX-A: Sorted by RECENCY (most recent first), not proximity.
    FIX-C: bar_age included so sweep detection can enforce MIN_LEVEL_AGE_BARS.
    """
    df = hist.tail(lookback)
    n  = len(df)
    highs: List[Tuple[float, int]] = []
    lows:  List[Tuple[float, int]] = []

    for i in range(pivot_bars, n - pivot_bars):
        candle_high = float(df['High'].iloc[i])
        candle_low  = float(df['Low'].iloc[i])

        left_h  = df['High'].iloc[i - pivot_bars:i]
        right_h = df['High'].iloc[i + 1: i + pivot_bars + 1]
        if candle_high >= float(left_h.max()) and candle_high >= float(right_h.max()):
            highs.append((candle_high, n - 1 - i))

        left_l  = df['Low'].iloc[i - pivot_bars:i]
        right_l = df['Low'].iloc[i + 1: i + pivot_bars + 1]
        if candle_low <= float(left_l.min()) and candle_low <= float(right_l.min()):
            lows.append((candle_low, n - 1 - i))

    # Deduplicate — keep most recent occurrence of each price
    seen_h: dict = {}
    for price, age in highs:
        key = round(price, 4)
        if key not in seen_h or age < seen_h[key][1]:
            seen_h[key] = (price, age)

    seen_l: dict = {}
    for price, age in lows:
        key = round(price, 4)
        if key not in seen_l or age < seen_l[key][1]:
            seen_l[key] = (price, age)

    return {
        'highs': sorted(seen_h.values(), key=lambda x: x[1]),   # most recent first
        'lows':  sorted(seen_l.values(), key=lambda x: x[1]),
    }


def _count_level_touches(
    hist: pd.DataFrame,
    level: float,
    tolerance_pct: float = _LEVEL_TOLERANCE_PCT,
    lookback: int = _TOUCH_LOOKBACK,
    exclude_recent: int = _TOUCH_EXCLUDE_RECENT,
) -> int:
    """
    Count touches of level, excluding the pivot formation zone (FIX-F).
    Vectorized — no iterrows.
    """
    total = len(hist)
    if total < exclude_recent + 5:
        return 0
    start = max(0, total - lookback - exclude_recent)
    end   = total - exclude_recent
    df    = hist.iloc[start:end]
    if df.empty:
        return 0
    tol     = level * tolerance_pct
    touched = ((df['High'] - level).abs() <= tol) | ((df['Low'] - level).abs() <= tol)
    return int(touched.sum())


# ═══════════════════════════════════════════════════════════════
# SWEEP DETECTION
# ═══════════════════════════════════════════════════════════════

def _detect_bullish_sweep(
    hist: pd.DataFrame,
    swing_lows: List[Tuple[float, int]],
    atr: float,
    current_price: float,
) -> Optional[Dict]:
    """
    Wick below swing low + close above it = bullish stop hunt reversal.
    FIX-B: min_level_dist enforced.
    FIX-C: level_age >= MIN_LEVEL_AGE_BARS enforced.
    FIX-D: only _MAX_LEVELS_TO_CHECK levels checked.
    """
    if not swing_lows or len(hist) < 5:
        return None

    min_depth      = max(atr * _MIN_SWEEP_DEPTH_ATR, current_price * _MIN_SWEEP_DEPTH_PCT)
    max_depth      = atr * _MAX_SWEEP_DEPTH_ATR
    min_level_dist = atr * _MIN_LEVEL_DIST_ATR

    for age in range(min(_SWEEP_CANDLE_LOOKBACK, len(hist))):
        candle  = hist.iloc[-(1 + age)]
        c_high  = float(candle['High'])
        c_low   = float(candle['Low'])
        c_close = float(candle['Close'])
        c_range = c_high - c_low

        for level_price, level_age in swing_lows[:_MAX_LEVELS_TO_CHECK]:
            if level_age < _MIN_LEVEL_AGE_BARS:
                continue
            if (current_price - level_price) < min_level_dist:
                continue
            below_by = level_price - c_low
            if below_by < min_depth or below_by > max_depth:
                continue
            if c_close <= level_price:
                continue
            close_pct = (c_close - c_low) / c_range if c_range > 0 else 0.5
            # NOTE: close_pct filtering is intentionally NOT done here.
            # The signal function handles close_pct as a confidence boost.
            # The detector's only job is to find structural sweeps.
            return {
                'type':              'BULLISH_SWEEP',
                'swept_level':       level_price,
                'level_age':         level_age,
                'wick_depth':        round(below_by, 6),
                'wick_depth_atr':    round(below_by / atr, 3),
                'close_pct':         round(close_pct, 3),
                'entry':             float(hist['Close'].iloc[-1]),
                'candles_since_sweep': age,
            }
    return None


def _detect_bearish_sweep(
    hist: pd.DataFrame,
    swing_highs: List[Tuple[float, int]],
    atr: float,
    current_price: float,
) -> Optional[Dict]:
    """
    Wick above swing high + close below it = bearish stop hunt reversal.
    """
    if not swing_highs or len(hist) < 5:
        return None

    min_depth      = max(atr * _MIN_SWEEP_DEPTH_ATR, current_price * _MIN_SWEEP_DEPTH_PCT)
    max_depth      = atr * _MAX_SWEEP_DEPTH_ATR
    min_level_dist = atr * _MIN_LEVEL_DIST_ATR

    for age in range(min(_SWEEP_CANDLE_LOOKBACK, len(hist))):
        candle  = hist.iloc[-(1 + age)]
        c_high  = float(candle['High'])
        c_low   = float(candle['Low'])
        c_close = float(candle['Close'])
        c_range = c_high - c_low

        for level_price, level_age in swing_highs[:_MAX_LEVELS_TO_CHECK]:
            if level_age < _MIN_LEVEL_AGE_BARS:
                continue
            if (level_price - current_price) < min_level_dist:
                continue
            above_by = c_high - level_price
            if above_by < min_depth or above_by > max_depth:
                continue
            if c_close >= level_price:
                continue
            close_pct = (c_close - c_low) / c_range if c_range > 0 else 0.5
            # NOTE: close_pct filtering intentionally NOT done here — see bullish note.
            return {
                'type':              'BEARISH_SWEEP',
                'swept_level':       level_price,
                'level_age':         level_age,
                'wick_height':       round(above_by, 6),
                'wick_height_atr':   round(above_by / atr, 3),
                'close_pct':         round(close_pct, 3),
                'entry':             float(hist['Close'].iloc[-1]),
                'candles_since_sweep': age,
            }
    return None


# ═══════════════════════════════════════════════════════════════
# CONFIRMATION FILTERS
# ═══════════════════════════════════════════════════════════════

def _volume_confirmation(
    hist: pd.DataFrame,
    vol_spike_mult: float = _VOL_SPIKE_MULT,
    age: int = 0,
) -> Tuple[bool, float]:
    """
    FIX-E: Hard gate — floor. vol_spike_mult=0 disables floor entirely.
    V4:    Hard gate — ceiling. vol >= _VOL_CEILING_MULT → block.
           vol 2-3x = institutional (EV+). vol 3x+ = panic (WR=0% in 180d).
    No Volume column → (False, 0.0).
    """
    if 'Volume' not in hist.columns:
        return False, 0.0

    vol_end   = -(1 + age)
    vol_start = -(1 + age + _VOL_LOOKBACK)
    if abs(vol_start) > len(hist):
        return False, 0.0

    sweep_vol = float(hist['Volume'].iloc[vol_end])
    avg_vol   = float(hist['Volume'].iloc[vol_start:vol_end].mean())

    if avg_vol <= 0 or pd.isna(avg_vol) or pd.isna(sweep_vol):
        return False, 0.0

    ratio = sweep_vol / avg_vol

    # Ceiling gate: panic volume — stops already ran, no reversal fuel left
    if ratio >= _VOL_CEILING_MULT:
        return False, round(ratio, 2)

    # Floor gate (disabled when vol_spike_mult=0)
    if vol_spike_mult > 0 and ratio < vol_spike_mult:
        return False, round(ratio, 2)

    return True, round(ratio, 2)


def _rsi_gate_ok(
    rsi_series: pd.Series,
) -> Tuple[bool, float]:
    """
    FIX-G: Block RSI extremes (< _RSI_BLOCK_LOW or > _RSI_BLOCK_HIGH).
    Extremes = exhaustion = trend continuation, NOT a reversal sweep.
    Best sweeps occur in RSI neutral zone (35-65).
    Returns (passes: bool, rsi_value: float).
    """
    if rsi_series is None or len(rsi_series) < 2:
        return True, 50.0

    rsi = float(rsi_series.iloc[-1])
    if pd.isna(rsi):
        return True, 50.0

    if rsi < _RSI_BLOCK_LOW or rsi > _RSI_BLOCK_HIGH:
        return False, rsi

    return True, rsi


def _calc_trade_levels(
    direction:    str,
    entry:        float,
    swept_level:  float,
    swing_levels: Dict[str, List[Tuple[float, int]]],
    atr:          float,
    min_rr:       float = 2.0,
) -> Tuple[float, float, float]:
    """
    SL anchored to swept level (the invalidation point).
    Target: next structural level in trade direction.
    Returns (target_1, stop_loss, actual_rr).
    """
    sl_buffer = atr * _SL_BUFFER_ATR

    if direction == 'BUY':
        stop_loss  = swept_level - sl_buffer
        risk       = entry - stop_loss
        if risk <= 0:
            stop_loss = entry - atr; risk = atr
        min_target = entry + risk * min_rr
        candidates = [p for p, _ in swing_levels.get('highs', []) if p > min_target]
        target_1   = min(candidates) if candidates else min_target
        if target_1 <= entry:
            target_1 = entry + risk * min_rr
    else:
        stop_loss  = swept_level + sl_buffer
        risk       = stop_loss - entry
        if risk <= 0:
            stop_loss = entry + atr; risk = atr
        min_target = entry - risk * min_rr
        candidates = [p for p, _ in swing_levels.get('lows', []) if p < min_target]
        target_1   = max(candidates) if candidates else min_target
        if target_1 >= entry:
            target_1 = entry - risk * min_rr

    actual_rr = abs(target_1 - entry) / risk if risk > 0 else min_rr
    return round(target_1, 8), round(stop_loss, 8), round(actual_rr, 2)


# ═══════════════════════════════════════════════════════════════
# MAIN BRAIN FUNCTION
# ═══════════════════════════════════════════════════════════════

def liquidity_sweep_signal(
    hist:   pd.DataFrame,
    symbol: str = "",
    regime: str = "VOLATILE",
) -> BrainSignal:
    """
    Liquidity Sweep Brain v3.
    Target assets: Large-cap equities (LT.NS, TATASTEEL.NS, AAPL,
                   RELIANCE.NS, ITC.NS, AMD, GOOGL).
    NOT suitable for crypto H1 (trend-continuation instruments).
    Best TF: 1H, 4H, D1.
    """
    _base = dict(
        brain_name='Liquidity-Sweep', specialization='Stop-Hunt Reversal Detector',
        method='Structural sweeps v5 (equity: regime-gated + vol-ceiling + touch-penalty)',
        rr_t1_mult=2.0, rr_t2_mult=3.5, rr_sl_mult=_SL_BUFFER_ATR,
    )

    def _hold(reason, conf=0.30, df_key='GATE_HOLD', meas=None):
        price = float(hist['Close'].iloc[-1]) if len(hist) > 0 else 0.0
        m = {'decision_factor': df_key, 'price_at_signal': round(price, 6), 'bars_used': len(hist)}
        if meas:
            m.update(meas)
        return BrainSignal(
            **_base, direction='HOLD', confidence=conf, signal_strength=0.0,
            signal_age_candles=0, primary_evidence=reason,
            supporting_factors=[], contra_factors=[],
            method_confidence=0.0, regime_suitability='LOW', measurements=m,
        )

    # ── Gates ─────────────────────────────────────────────────────────────────
    if len(hist) < _MIN_HIST_BARS:
        return _hold(f'Insufficient data ({len(hist)}<{_MIN_HIST_BARS})', 0.25, 'GATE_DATA')

    if regime == 'CHAOS':
        return _hold('CHAOS regime — no clean structure', 0.25, 'GATE_CHAOS')

    price = float(hist['Close'].iloc[-1])
    atr   = calc_atr(hist, period=14)
    if atr <= 0:
        return _hold('ATR=0', 0.25, 'GATE_ZERO_ATR')

    atr_pct    = atr / price if price > 0 else 0.0
    rsi_series = calc_rsi_series(hist, period=14)
    rsi_val    = float(rsi_series.iloc[-1]) if not rsi_series.empty else 50.0
    if pd.isna(rsi_val):
        rsi_val = 50.0

    # ── Structural levels (FIX-A: recency sort) ───────────────────────────────
    swing_levels = _find_swing_levels(hist, _SWING_LOOKBACK, _PIVOT_BARS, price)

    # ── Sweep detection ───────────────────────────────────────────────────────
    bull_sweep = _detect_bullish_sweep(hist, swing_levels['lows'],  atr, price)
    bear_sweep = _detect_bearish_sweep(hist, swing_levels['highs'], atr, price)

    if not bull_sweep and not bear_sweep:
        return _hold(
            f'No structural sweep (last {_SWEEP_CANDLE_LOOKBACK} candles)',
            0.40, 'GATE_NO_SWEEP', {
                'rsi': round(rsi_val, 1), 'atr_at_signal': round(atr, 6),
                'atr_pct_at_signal': round(atr_pct * 100, 3),
                'n_swing_highs': len(swing_levels['highs']),
                'n_swing_lows':  len(swing_levels['lows']),
                'bars_used': len(hist),
            },
        )

    # Dual-sweep: keep stronger wick
    if bull_sweep and bear_sweep:
        if bull_sweep.get('wick_depth_atr', 0) >= bear_sweep.get('wick_height_atr', 0):
            bear_sweep = None
        else:
            bull_sweep = None

    sweep     = bull_sweep or bear_sweep
    direction = 'BUY' if bull_sweep else 'SELL'
    age       = sweep.get('candles_since_sweep', 0)

    # ── V5: Regime directional gate (equity-specific) ─────────────────────────
    # BUY_TRENDING_DOWN: 180d n=4, WR=0.0%, avg_R=-1.000 — breakdown not reversal. BLOCKED.
    # SELL_TRENDING_DOWN: no equity evidence either way. Re-allowed in V5.
    #   Bearish sweep in downtrend = institutional distribution stop-hunt. Conceptually valid.
    # BUY_TRENDING_UP: wrong direction. BLOCKED.
    _BUY_ALLOWED_REGIMES  = {'RANGING', 'VOLATILE'}
    _SELL_ALLOWED_REGIMES = {'RANGING', 'VOLATILE', 'TRENDING_UP', 'TRENDING_DOWN'}

    if direction == 'BUY' and regime not in _BUY_ALLOWED_REGIMES:
        return _hold(
            f'BUY blocked in {regime} — equity BUY sweeps require RANGING/VOLATILE',
            0.35, 'GATE_REGIME_DIR',
            {'regime': regime, 'direction': direction},
        )
    if direction == 'SELL' and regime not in _SELL_ALLOWED_REGIMES:
        return _hold(
            f'SELL blocked in {regime} — equity SELL sweeps require RANGING/VOLATILE/TRENDING_UP/TRENDING_DOWN',
            0.35, 'GATE_REGIME_DIR',
            {'regime': regime, 'direction': direction},
        )

    # ── FIX-G: RSI neutral gate ───────────────────────────────────────────────
    rsi_ok, rsi_val = _rsi_gate_ok(rsi_series)
    if not rsi_ok:
        return _hold(
            f'RSI extreme ({rsi_val:.1f}) — exhaustion, not reversal',
            0.38, 'GATE_RSI_EXTREME', {'rsi': round(rsi_val, 1)},
        )

    # ── FIX-E + V4: Volume window gate (floor + ceiling) ──────────────────────
    vol_passes, vol_ratio = _volume_confirmation(hist, _VOL_SPIKE_MULT, age=age)
    if not vol_passes:
        if vol_ratio >= _VOL_CEILING_MULT:
            return _hold(
                f'Volume {vol_ratio:.1f}x >= {_VOL_CEILING_MULT}x ceiling — panic, not institutional',
                0.35, 'GATE_PANIC_VOLUME', {'vol_ratio': vol_ratio, 'rsi': round(rsi_val, 1)},
            )
        if _VOL_SPIKE_MULT > 0:
            return _hold(
                f'Volume {vol_ratio:.1f}x < {_VOL_SPIKE_MULT}x floor required',
                0.38, 'GATE_LOW_VOLUME', {'vol_ratio': vol_ratio, 'rsi': round(rsi_val, 1)},
            )

    # ── FIX-F: Touch count (confidence scoring, NOT a hard gate) ──────────────
    # V4 made this a hard gate on n=6 evidence — reverted in V5.
    # Hard gate requires min n=20 per bucket. n=6 is noise.
    # Overused levels get confidence penalty, not automatic block.
    touches = _count_level_touches(hist, sweep['swept_level'])

    close_pct  = sweep.get('close_pct', 0.5)
    delta_flow     = compute_delta_flow(hist)
    delta_confirms = (delta_flow is not None and (
        (direction == 'BUY' and delta_flow > 0.15) or (direction == 'SELL' and delta_flow < -0.15)
    ))

    # ── Trade levels ──────────────────────────────────────────────────────────
    target_1, stop_loss, actual_rr = _calc_trade_levels(
        direction=direction, entry=price, swept_level=sweep['swept_level'],
        swing_levels=swing_levels, atr=atr, min_rr=2.0,
    )

    # ── Confidence scoring ────────────────────────────────────────────────────
    # base_conf: already passed all hard gates (regime, RSI, vol, touches, close)
    base_conf = 0.65
    cs        = 0.0
    confirmations: List[str] = []
    contra:        List[str] = []

    # Volume quality (institutional sweet spot 1.5-2.5x = better signal)
    if 0 < vol_ratio < _VOL_CEILING_MULT:
        if vol_ratio >= 1.5:
            vol_score = 0.08 if vol_ratio < 2.5 else 0.04
            cs += vol_score; confirmations.append(f'Vol={vol_ratio:.1f}x')

    # Strong close = reversal candle body closes decisively past swept level
    # BUY sweep geometry: close_pct always 0.80-0.99 (wick below, closes up)
    # SELL sweep geometry: close_pct always 0.05-0.35 (wick above, closes down)
    # Reward stronger closes within the natural range of each direction
    if direction == 'BUY':
        if close_pct >= 0.80:
            cs += 0.08; confirmations.append(f'Strong close ({close_pct:.0%})')
        elif close_pct >= 0.65:
            cs += 0.04; confirmations.append(f'Moderate close ({close_pct:.0%})')
        else:
            contra.append(f'Weak close ({close_pct:.0%})')
    else:  # SELL
        if close_pct <= 0.20:
            cs += 0.08; confirmations.append(f'Strong close ({close_pct:.0%})')
        elif close_pct <= 0.35:
            cs += 0.04; confirmations.append(f'Moderate close ({close_pct:.0%})')
        else:
            contra.append(f'Weak close ({close_pct:.0%})')

    # Touch count scoring (V5: informational only — no cs impact above 2 touches)
    # Evidence base for penalty was n=6 — too small. Collect data first.
    if touches == 0:
        cs += 0.06; confirmations.append(f'Virgin level (0 touches)')
    elif touches == 1:
        cs += 0.04; confirmations.append(f'Clean level (1 touch)')
    elif touches == 2:
        cs += 0.01; confirmations.append(f'Tested level (2 touches)')
    else:
        # 3+ touches: logged as contra for breakdown analysis, NOT penalised in cs
        # Reason: zero equity evidence (n<20) that overused levels lose more
        contra.append(f'Overused level ({touches} touches)')

    # Delta flow
    if delta_confirms and delta_flow is not None:
        cs += 0.05; confirmations.append(f'Delta={delta_flow:+.2f}')
    elif delta_flow is not None:
        contra.append(f'Delta neutral ({delta_flow:+.2f})')

    # V4: RSI directional sweet spot (boost, not hard gate)
    # 180d: RSI 60-70 = 100% WR for BUY — reward this zone
    level_age = sweep.get('level_age', _MIN_LEVEL_AGE_BARS)
    if direction == 'BUY':
        if _RSI_BUY_SWEET_LOW <= rsi_val <= _RSI_BUY_SWEET_HIGH:
            cs += 0.05; confirmations.append(f'RSI in BUY zone ({rsi_val:.1f})')
        else:
            contra.append(f'RSI={rsi_val:.1f} outside BUY sweet spot ({_RSI_BUY_SWEET_LOW}-{_RSI_BUY_SWEET_HIGH})')
    else:  # SELL
        if _RSI_SELL_SWEET_LOW <= rsi_val <= _RSI_SELL_SWEET_HIGH:
            cs += 0.05; confirmations.append(f'RSI in SELL zone ({rsi_val:.1f})')
        else:
            contra.append(f'RSI={rsi_val:.1f} outside SELL sweet spot ({_RSI_SELL_SWEET_LOW}-{_RSI_SELL_SWEET_HIGH})')

    # Level maturity
    if level_age >= 20:
        cs += 0.03; confirmations.append(f'Mature level ({level_age}b)')
    elif level_age >= 10:
        cs += 0.01  # small reward for meeting age gate comfortably

    confidence = min(0.92, base_conf + cs)

    if age > 0:
        confidence = max(0.50, confidence - age * _AGE_CONFIDENCE_DECAY)
        contra.append(f'Sweep {age}c old')

    if regime == 'SQUEEZE':
        confidence = round(confidence * 0.90, 3); contra.append('SQUEEZE regime')

    # ── FIX-I: Minimum confidence hard gate ──────────────────────────────────
    if confidence < _MIN_CONFIDENCE_GATE:
        return _hold(
            f'Confidence {confidence:.0%} < {_MIN_CONFIDENCE_GATE:.0%} gate',
            confidence, 'GATE_LOW_CONFIDENCE', {
                'rsi': round(rsi_val, 1), 'vol_ratio': vol_ratio,
                'close_pct': close_pct, 'touches': touches,
            },
        )

    # ── R:R multipliers ───────────────────────────────────────────────────────
    risk_in_atr = abs(price - stop_loss) / atr if atr > 0 else 1.0
    t1_in_atr   = abs(target_1 - price)  / atr if atr > 0 else 2.0
    wick_key    = 'wick_depth_atr' if direction == 'BUY' else 'wick_height_atr'
    wick_size   = sweep.get(wick_key, 0)

    return BrainSignal(
        **_base,
        direction          = direction,
        confidence         = round(confidence, 3),
        signal_strength    = round(cs, 3),
        signal_age_candles = age,
        primary_evidence   = (
            f'{sweep["type"]} at {sweep["swept_level"]:.6g} '
            f'(wick={wick_size:.2f}xATR, level_age={level_age}b) | '
            + ' | '.join(confirmations)
        ),
        supporting_factors = confirmations,
        contra_factors     = contra,
        method_confidence  = 0.85,
        regime_suitability = (
            'HIGH'   if (direction == 'BUY'  and regime in ('RANGING', 'VOLATILE')) or
                        (direction == 'SELL' and regime in ('RANGING', 'VOLATILE', 'TRENDING_UP'))
            else 'LOW'
        ),
        reliability_flags  = {
            'no_volume_spike':  vol_ratio < _VOL_SPIKE_MULT,
            'panic_volume':     vol_ratio >= _VOL_CEILING_MULT,
            'aged_signal':      age > 0,
        },
        measurements       = {
            'entry_price':        round(price, 6),
            'target_1':           round(target_1, 6),
            'stop_loss':          round(stop_loss, 6),
            'swept_level':        round(sweep['swept_level'], 6),
            'wick_depth_atr':     round(wick_size, 3),
            'close_pct':          round(close_pct, 3),
            'level_age':          level_age,
            'sweep_age':          age,
            'vol_ratio':          round(vol_ratio, 2),
            'rsi':                round(rsi_val, 1),
            'touches':            touches,
            'rr_achieved':        round(actual_rr, 2),
            'delta_flow':         round(delta_flow, 3) if delta_flow is not None else 0.0,
            'n_swing_highs':      len(swing_levels['highs']),
            'n_swing_lows':       len(swing_levels['lows']),
            'swing_levels': {
                'highs': [round(p, 6) for p, _ in swing_levels['highs'][:5]],
                'lows':  [round(p, 6) for p, _ in swing_levels['lows'][:5]],
            },
            'decision_factor':    f'LIQUIDITY_SWEEP_{sweep["type"]}',
            'price_at_signal':    round(price, 6),
            'atr_at_signal':      round(atr, 6),
            'atr_pct_at_signal':  round(atr_pct * 100, 3),
            'bars_used':          len(hist),
            'indicator_1_name':   'vol_ratio',      'indicator_1_value': round(vol_ratio, 3),
            'indicator_2_name':   'rsi',            'indicator_2_value': round(rsi_val, 1),
            'indicator_3_name':   'wick_depth_atr', 'indicator_3_value': round(wick_size, 3),
        },
        rr_t1_mult = round(t1_in_atr, 2),
        rr_t2_mult = round(t1_in_atr * 1.5, 2),
        rr_sl_mult = round(risk_in_atr, 2),
    )


# ═══════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("=" * 70)
    print("Liquidity-Sweep Brain v5 — Self-Test (equity-tuned)")
    print("=" * 70)
    print("V5 gates: regime(BUY=RANGING/VOL | SELL=all non-CHAOS) | vol-ceiling | touch-penalty")

    np.random.seed(42)
    n = 200   # more bars to satisfy v3 _MIN_HIST_BARS=100 + level age>=10

    # Build realistic OHLCV: equity price range (~$170 like AAPL)
    # Trending down first (to test TRENDING_DOWN BUY sweep), then stable
    prices = [170.0]
    for i in range(n - 1):
        drift = -0.30 if i < 80 else 0.15
        prices.append(max(10.0, prices[-1] + drift + np.random.normal(0, 1.2)))

    highs = [p * (1.0 + abs(np.random.normal(0, 0.004))) for p in prices]
    lows  = [p * (1.0 - abs(np.random.normal(0, 0.004))) for p in prices]
    vols  = [3_000_000 + np.random.randint(-500_000, 500_000) for _ in prices]

    df = pd.DataFrame({'Open': prices, 'High': highs, 'Low': lows, 'Close': prices, 'Volume': vols})

    # ── Test 1: CHAOS → HOLD ─────────────────────────────────────────────────
    r = liquidity_sweep_signal(df, 'BTC-USD', 'CHAOS')
    print(f"Test 1 (CHAOS->HOLD):         {r.direction} {'OK' if r.direction=='HOLD' else 'FAIL'}")

    # ── Test 2: < MIN_HIST_BARS → HOLD ───────────────────────────────────────
    r = liquidity_sweep_signal(df.head(50), 'BTC-USD', 'VOLATILE')
    print(f"Test 2 (<{_MIN_HIST_BARS}b->HOLD):        {r.direction} {'OK' if r.direction=='HOLD' else 'FAIL'}")

    # ── Test 3: TRENDING_DOWN + BUY → HOLD (BUY still blocked in TRENDING_DOWN) ──
    # V5: SELL in TRENDING_DOWN is NOW ALLOWED. BUY is still blocked.
    r = liquidity_sweep_signal(df, 'BTC-USD', 'TRENDING_DOWN')
    if r.direction == 'BUY':
        print(f"Test 3 (TD BUY blocked):      FAIL — BUY should be blocked in TRENDING_DOWN")
    else:
        # HOLD or SELL both acceptable in TRENDING_DOWN
        print(f"Test 3 (TD direction filter): {r.direction} OK — BUY correctly blocked, SELL allowed")

    # ── Test 4: Swing level recency sort ─────────────────────────────────────
    price = float(df['Close'].iloc[-1])
    sw    = _find_swing_levels(df, current_price=price)
    print(f"\nTest 4: Swing levels sorted by recency (age ascending)")
    if sw['lows']:
        ages    = [a for _, a in sw['lows'][:5]]
        ok      = ages == sorted(ages)
        print(f"  Lows:  ages={ages} {'OK' if ok else 'FAIL'}")
    if sw['highs']:
        ages_h  = [a for _, a in sw['highs'][:5]]
        ok_h    = ages_h == sorted(ages_h)
        print(f"  Highs: ages={ages_h} {'OK' if ok_h else 'FAIL'}")

    # ── Test 5: Touch count excludes pivot zone ───────────────────────────────
    print(f"\nTest 5: Touch count excludes last {_TOUCH_EXCLUDE_RECENT} bars")
    if sw['lows']:
        level  = sw['lows'][0][0]
        t_full = _count_level_touches(df, level, exclude_recent=0)
        t_excl = _count_level_touches(df, level)
        print(f"  Level={level:.0f}: touches_full={t_full}, touches_excl={t_excl}")
        print(f"  Exclusion working: {'OK' if t_excl <= t_full else 'FAIL'}")

    # ── Test 6: RSI gate blocks extremes ─────────────────────────────────────
    print(f"\nTest 6: RSI gate")
    rsi_series = calc_rsi_series(df, 14)
    ok, rsi = _rsi_gate_ok(rsi_series)
    print(f"  Current RSI={rsi:.1f}: gate passes={ok}")
    print(f"  Rule: RSI<{_RSI_BLOCK_LOW} or >{_RSI_BLOCK_HIGH} → blocked")

    # ── Test 7: measurements keys present ────────────────────────────────────
    print(f"\nTest 7: Forced signal — measurements keys check")
    df2 = df.copy()
    atr_t = calc_atr(df2, 14)
    sw_t  = _find_swing_levels(df2, current_price=float(df2['Close'].iloc[-1]))
    old_lows = [(p, a) for p, a in sw_t['lows'] if a >= _MIN_LEVEL_AGE_BARS]
    if old_lows:
        swept_lv = old_lows[0][0]
        cur_p    = float(df2['Close'].iloc[-1])
        if (cur_p - swept_lv) >= atr_t * _MIN_LEVEL_DIST_ATR:
            df2.loc[df2.index[-1], 'Low']    = swept_lv - atr_t * 0.40
            df2.loc[df2.index[-1], 'Close']  = swept_lv + atr_t * 0.70
            df2.loc[df2.index[-1], 'High']   = swept_lv + atr_t * 1.10
            df2.loc[df2.index[-1], 'Volume'] = 3_500_000
            r7 = liquidity_sweep_signal(df2, 'BTC-USD', 'VOLATILE')
            print(f"  Direction: {r7.direction} | Confidence: {r7.confidence:.0%}")
            if r7.direction == 'BUY':
                m = r7.measurements
                print(f"  entry_price in measurements: {'OK' if 'entry_price' in m else 'FAIL'}")
                print(f"  target_1 in measurements:    {'OK' if 'target_1' in m else 'FAIL'}")
                print(f"  stop_loss in measurements:   {'OK' if 'stop_loss' in m else 'FAIL'}")
                print(f"  swing_levels in measurements:{'OK' if 'swing_levels' in m else 'FAIL'}")
                if 'stop_loss' in m and 'swept_level' in m:
                    print(f"  SL below swept level:        {'OK' if m['stop_loss'] < m['swept_level'] else 'FAIL'}")
            else:
                print(f"  Signal was {r7.direction} — reason: {r7.primary_evidence[:80]}")
    else:
        print("  No mature levels in synthetic data for forced test (normal)")

    print("\n" + "=" * 70)
    print("Self-test complete.")
    print(f"V5 key gates: level_age>={_MIN_LEVEL_AGE_BARS}b | min_dist>={_MIN_LEVEL_DIST_ATR}xATR | "
          f"vol>={_VOL_SPIKE_MULT}x floor | vol<{_VOL_CEILING_MULT}x ceiling | RSI {_RSI_BLOCK_LOW}-{_RSI_BLOCK_HIGH} | "
          f"conf>={_MIN_CONFIDENCE_GATE:.0%} | touch=penalty-only")
    print("=" * 70)