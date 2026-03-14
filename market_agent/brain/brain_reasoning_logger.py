"""
brain_reasoning_logger.py
==========================
Plain-English decision logger for the Liquidity-Sweep brain.

PURPOSE:
  Every brain decision (HOLD or BUY/SELL) is translated into a human-readable
  explanation covering:
    1. What pattern was detected (or why none was found)
    2. What each filter checked and whether it passed or failed
    3. What confirms the trade and what argues against it
    4. Final verdict and confidence score with plain-English meaning

USAGE:
  from market_agent.brain.brain_reasoning_logger import explain_signal

  signal = liquidity_sweep_signal(hist, symbol, regime)
  explanation = explain_signal(signal, symbol, regime, bar_time)
  print(explanation)           # print to terminal
  logger.info(explanation)     # write to log file

ANSWERS YOUR QUESTION ABOUT BUYERS vs SELLERS:
  This brain does NOT use order book data (bid/ask depth, Level 2).
  It uses PRICE ACTION + VOLUME to infer institutional intent:
    - A wick sweep = price briefly broke through a level (stops triggered)
    - A strong close back = the sweeping party (institution) reversed direction
    - Volume spike = confirms institutional size was behind the move
    - Delta flow (if available) = net buy/sell pressure from candle internals
  This is a structural reversal pattern, not a real-time order flow brain.
  A separate order-flow brain would use actual bid/ask imbalance data.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from market_agent.brain.brain_contract import BrainSignal


# ═══════════════════════════════════════════════════════════════
# CONFIDENCE LEVEL DESCRIPTIONS
# ═══════════════════════════════════════════════════════════════

def _confidence_label(conf: float) -> str:
    if conf >= 0.85: return "VERY HIGH"
    if conf >= 0.75: return "HIGH"
    if conf >= 0.65: return "MODERATE"
    if conf >= 0.55: return "LOW"
    return "VERY LOW"


def _rr_quality(rr: float) -> str:
    if rr >= 4.0: return "excellent"
    if rr >= 3.0: return "strong"
    if rr >= 2.0: return "acceptable"
    if rr >= 1.5: return "marginal"
    return "poor"


# ═══════════════════════════════════════════════════════════════
# MAIN EXPLAINER
# ═══════════════════════════════════════════════════════════════

def explain_signal(
    signal:   BrainSignal,
    symbol:   str,
    regime:   str,
    bar_time: Optional[datetime] = None,
) -> str:
    """
    Convert a BrainSignal into a complete plain-English explanation.

    Returns a multi-line string suitable for logging or display.
    """
    m    = signal.measurements or {}
    time_str = bar_time.strftime('%Y-%m-%d %H:%M') if bar_time else 'unknown time'
    sep  = "─" * 68

    lines = []
    lines.append(sep)
    lines.append(f"LIQUIDITY-SWEEP BRAIN DECISION  |  {symbol}  |  {time_str}")
    lines.append(sep)

    # ── VERDICT ─────────────────────────────────────────────────────────────
    direction = signal.direction
    conf      = signal.confidence or 0.0
    conf_lbl  = _confidence_label(conf)

    if direction == 'HOLD':
        lines.append(f"VERDICT: HOLD  (no trade)")
        lines.append(f"Confidence: {conf:.0%}  [{conf_lbl}]")
    elif direction == 'BUY':
        lines.append(f"VERDICT: BUY  →  Go LONG on {symbol}")
        lines.append(f"Confidence: {conf:.0%}  [{conf_lbl}]")
    else:
        lines.append(f"VERDICT: SELL  →  Go SHORT on {symbol}")
        lines.append(f"Confidence: {conf:.0%}  [{conf_lbl}]")

    lines.append("")

    # ── HOLD REASON (early exit) ─────────────────────────────────────────────
    if direction == 'HOLD':
        reason      = signal.primary_evidence or "no reason provided"
        gate_key    = m.get('decision_factor', '')
        lines.append("WHY HOLD:")
        lines.append(_explain_hold_reason(reason, gate_key, m, regime))
        lines.append(sep)
        return "\n".join(lines)

    # ── PATTERN DETECTED ─────────────────────────────────────────────────────
    lines.append("PATTERN DETECTED:")
    swept   = m.get('swept_level', 0)
    wick    = m.get('wick_depth_atr', 0)
    age_lvl = m.get('level_age', 0)
    age_swp = m.get('sweep_age', 0)
    price   = m.get('entry_price', 0)
    atr     = m.get('atr_at_signal', 0)
    close_p = m.get('close_pct', 0)

    if direction == 'BUY':
        lines.append(
            f"  Bullish stop-hunt reversal detected on {symbol}."
        )
        lines.append(
            f"  A candle wick pierced BELOW the swing low at {swept:.4g}, "
            f"sweeping the stops of traders who were long near that level."
        )
        lines.append(
            f"  The candle then CLOSED BACK ABOVE the level — this is the "
            f"institutional reversal signal. The 'smart money' ran the stops "
            f"and immediately reversed direction to buy."
        )
        close_meaning = (
            "strongly" if close_p >= 0.80 else
            "moderately" if close_p >= 0.65 else
            "weakly"
        )
        lines.append(
            f"  Close position: {close_p:.0%} of candle range "
            f"({close_meaning} in the upper half — typical for bullish sweeps)."
        )
    else:
        lines.append(
            f"  Bearish stop-hunt reversal detected on {symbol}."
        )
        lines.append(
            f"  A candle wick pierced ABOVE the swing high at {swept:.4g}, "
            f"sweeping the stops of traders who were short near that level."
        )
        lines.append(
            f"  The candle then CLOSED BACK BELOW the level — institutional "
            f"reversal signal. Sellers swept the stops and reversed to sell."
        )
        close_meaning = (
            "strongly" if close_p <= 0.20 else
            "moderately" if close_p <= 0.35 else
            "weakly"
        )
        lines.append(
            f"  Close position: {close_p:.0%} of candle range "
            f"({close_meaning} in the lower half — typical for bearish sweeps)."
        )

    lines.append(
        f"  Wick size: {wick:.2f}x ATR  |  "
        f"Level age: {age_lvl} bars old  |  "
        f"Sweep age: {'this candle' if age_swp == 0 else str(age_swp) + ' candle(s) ago'}"
    )
    lines.append("")

    # ── MARKET CONTEXT ───────────────────────────────────────────────────────
    lines.append("MARKET CONTEXT:")
    lines.append(_explain_regime(regime, direction))

    rsi = m.get('rsi', 50)
    lines.append(_explain_rsi(rsi, direction))

    vol = m.get('vol_ratio', 0)
    lines.append(_explain_volume(vol, direction))

    delta = m.get('delta_flow', None)
    if delta is not None:
        lines.append(_explain_delta(delta, direction))

    touches = m.get('touches', 0)
    lines.append(_explain_touches(touches))
    lines.append("")

    # ── WHAT THIS BRAIN DOES AND DOES NOT USE ───────────────────────────────
    lines.append("WHAT SIGNALS THIS BRAIN USES:")
    lines.append(
        "  ✓ Price action (wick sweeps past structural levels → reversals)"
    )
    lines.append(
        "  ✓ Volume (confirms institutional size; ceiling blocks panic moves)"
    )
    lines.append(
        "  ✓ RSI (blocks extreme readings where reversals rarely hold)"
    )
    lines.append(
        "  ✓ Regime (market trend context — BUY only in RANGING/VOLATILE)"
    )
    lines.append(
        "  ✓ Delta flow (net buy vs sell pressure from candle body/wick ratio)"
    )
    lines.append(
        "  ✗ Order book depth (bid/ask imbalance — NOT used by this brain)"
    )
    lines.append(
        "  ✗ Level 2 data (market microstructure — separate brain needed)"
    )
    lines.append(
        "  NOTE: Delta flow is the closest this brain gets to 'buyers vs sellers'."
    )
    lines.append(
        "  It measures whether the candle closed with net buying or selling pressure."
    )
    lines.append("")

    # ── SUPPORTING FACTORS ───────────────────────────────────────────────────
    if signal.supporting_factors:
        lines.append("SUPPORTING EVIDENCE (reasons TO take this trade):")
        for factor in signal.supporting_factors:
            lines.append(f"  + {_expand_factor(factor)}")
        lines.append("")

    # ── CONTRA FACTORS ───────────────────────────────────────────────────────
    if signal.contra_factors:
        lines.append("CONTRA EVIDENCE (reasons to be cautious):")
        for factor in signal.contra_factors:
            lines.append(f"  - {_expand_contra(factor)}")
        lines.append("")

    # ── TRADE LEVELS ────────────────────────────────────────────────────────
    entry  = m.get('entry_price',  price)
    target = m.get('target_1',     0)
    sl     = m.get('stop_loss',    0)
    rr     = m.get('rr_achieved',  0)

    lines.append("TRADE LEVELS:")
    lines.append(f"  Entry:      {entry:.4g}")
    lines.append(f"  Target T1:  {target:.4g}  (next structural level in trade direction)")
    lines.append(f"  Stop Loss:  {sl:.4g}  (anchored to swept level with {m.get('atr_pct_at_signal',0):.2f}% ATR buffer)")
    lines.append(f"  R:R ratio:  {rr:.1f}:1  [{_rr_quality(rr)}]")
    if atr > 0:
        risk_pct = abs(entry - sl) / entry * 100
        lines.append(f"  Risk:       {risk_pct:.2f}% of entry price")
    lines.append("")

    # ── CONFIDENCE BREAKDOWN ─────────────────────────────────────────────────
    lines.append(f"CONFIDENCE BREAKDOWN: {conf:.0%} [{conf_lbl}]")
    lines.append(f"  Base score:          65%  (passed all required gates)")
    boost = round((conf - 0.65) * 100, 1)
    lines.append(f"  Evidence boost:     +{boost:.1f}%  (from supporting factors above)")
    lines.append(
        f"  Interpretation: "
        + (
            "Strong setup — multiple factors align."
            if conf >= 0.80 else
            "Decent setup — core pattern valid, some uncertainty."
            if conf >= 0.70 else
            "Marginal setup — passed gates but limited confirmation."
        )
    )
    lines.append(sep)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# HOLD REASON EXPLAINER
# ═══════════════════════════════════════════════════════════════

def _explain_hold_reason(reason: str, gate_key: str, m: dict, regime: str) -> str:
    """Translate HOLD gate codes into plain English."""

    if 'GATE_DATA' in gate_key or 'Insufficient' in reason:
        return (
            f"  NOT ENOUGH DATA: Brain requires at least "
            f"{m.get('bars_used', '?')} bars of history to detect structural "
            f"levels reliably. More historical data needed."
        )

    if 'GATE_CHAOS' in gate_key or 'CHAOS' in reason:
        return (
            "  CHAOTIC MARKET: The regime classifier has identified a CHAOS "
            "regime — price is moving without structural logic. Stop-hunt "
            "reversals do not work in CHAOS (no clean levels to sweep). "
            "Waiting for market structure to re-establish."
        )

    if 'GATE_REGIME_DIR' in gate_key or 'regime' in reason.lower():
        direction = m.get('direction', '')
        if direction == 'BUY':
            return (
                f"  WRONG REGIME FOR BUY: Market is in {regime}. "
                f"BUY sweeps only work in RANGING or VOLATILE regimes where "
                f"price is mean-reverting around structure. In a trending market, "
                f"a sweep of a swing low is a BREAKDOWN, not a reversal. "
                f"Waiting for market to enter a ranging or choppy phase."
            )
        else:
            return (
                f"  WRONG REGIME FOR SELL: Market is in {regime}. "
                f"SELL sweeps work in RANGING, VOLATILE, TRENDING_UP, and "
                f"TRENDING_DOWN. Currently blocked by regime gate."
            )

    if 'GATE_NO_SWEEP' in gate_key or 'No structural sweep' in reason:
        return (
            "  NO PATTERN FOUND: No candle has swept through a swing high or "
            "low and closed back past it within the lookback window. The core "
            "stop-hunt pattern is simply not present right now. "
            "Brain is waiting for a wick that pierces a key structural level."
        )

    if 'GATE_RSI_EXTREME' in gate_key or 'RSI extreme' in reason:
        rsi = m.get('rsi', 50)
        if rsi > 70:
            return (
                f"  RSI TOO HIGH ({rsi:.1f}): When RSI is above 70, price is "
                f"already overbought. A sweep in this zone is usually exhaustion "
                f"or continuation — not a genuine reversal. "
                f"Brain waits for RSI to return below 70 before trusting a sweep."
            )
        else:
            return (
                f"  RSI TOO LOW ({rsi:.1f}): When RSI is below 30, price is "
                f"already oversold and can keep falling. A sweep here is likely "
                f"continuation, not reversal. Waiting for RSI above 30."
            )

    if 'GATE_PANIC_VOLUME' in gate_key or 'panic' in reason.lower():
        vol = m.get('vol_ratio', 0)
        return (
            f"  PANIC VOLUME ({vol:.1f}x average): Volume is more than 3x the "
            f"20-bar average. This is panic/capitulation activity — the "
            f"institutional stops have already run and the move is likely to "
            f"continue, not reverse. Brain confirmed this from backtest data: "
            f"vol ≥ 3x had WR=0% across 180 days. Waiting for calmer conditions."
        )

    if 'GATE_LOW_VOLUME' in gate_key or 'Volume' in reason:
        vol = m.get('vol_ratio', 0)
        return (
            f"  INSUFFICIENT VOLUME ({vol:.1f}x average): The sweep candle "
            f"did not have enough volume to confirm institutional participation. "
            f"A sweep with low volume could be a false break — retail noise, "
            f"not an institutional stop-hunt. Minimum required: volume floor set "
            f"by current parameter. A genuine stop-hunt requires above-average "
            f"volume to confirm that large orders were executed."
        )

    if 'GATE_LOW_CONFIDENCE' in gate_key or 'Confidence' in reason:
        conf = m.get('confidence', 0.0)
        return (
            f"  LOW CONFIDENCE ({conf:.0%}): The sweep was detected and basic "
            f"gates passed, but the combination of supporting and contra factors "
            f"produced a confidence score below the 65% minimum threshold. "
            f"Possible causes: weak volume, no delta confirmation, RSI outside "
            f"optimal zone. Not enough evidence to justify entering a position."
        )

    # Generic fallback
    return f"  BLOCKED: {reason}"


# ═══════════════════════════════════════════════════════════════
# CONTEXT EXPLAINERS
# ═══════════════════════════════════════════════════════════════

def _explain_regime(regime: str, direction: str) -> str:
    descriptions = {
        'RANGING':      "Price is moving sideways between support and resistance. "
                        "This is the ideal regime for BUY sweeps — institutions "
                        "hunt stops at the boundaries before reversing into range.",
        'VOLATILE':     "Price is moving sharply in both directions with no clear trend. "
                        "Stop-hunts are common as the market tests both sides. "
                        "Both BUY and SELL sweeps can work here.",
        'TRENDING_UP':  "Market is in an uptrend. SELL sweeps (wick above a high) "
                        "work here — distribution stop-hunts before downward pullbacks.",
        'TRENDING_DOWN':"Market is in a downtrend. SELL sweeps can still trigger "
                        "as institutions distribute. BUY sweeps in this regime "
                        "are blocked — sweeps of swing lows in downtrends are "
                        "usually breakdowns, not reversals.",
        'SQUEEZE':      "Volatility is compressed. Price is coiling. Stop-hunts "
                        "can happen before breakouts. Confidence reduced in squeeze.",
    }
    desc = descriptions.get(regime, f"Regime is {regime}.")
    return f"  Regime: {regime} — {desc}"


def _explain_rsi(rsi: float, direction: str) -> str:
    if rsi >= 60 and direction == 'BUY':
        return (
            f"  RSI: {rsi:.1f} — Elevated but not extreme. Backtest data shows "
            f"RSI 55-70 on BUY sweeps had the highest win rate. "
            f"Price has momentum but not yet overbought."
        )
    if rsi <= 40 and direction == 'SELL':
        return (
            f"  RSI: {rsi:.1f} — Depressed but not extreme. RSI 30-45 on SELL "
            f"sweeps showed good results — oversold enough for a bounce but "
            f"not so oversold that the trend is exhausted upward."
        )
    if 40 <= rsi <= 60:
        return (
            f"  RSI: {rsi:.1f} — Neutral zone. Neither overbought nor oversold. "
            f"Price has room to move in both directions. Neutral for this trade."
        )
    return (
        f"  RSI: {rsi:.1f} — Outside the directional sweet spot but within "
        f"the allowed range (30-70). Not ideal but not a blocker."
    )


def _explain_volume(vol: float, direction: str) -> str:
    if vol == 0:
        return (
            "  Volume: No volume data available or volume gate disabled. "
            "Cannot confirm institutional participation from volume alone."
        )
    if vol >= 2.0:
        return (
            f"  Volume: {vol:.1f}x the 20-bar average — strong institutional "
            f"participation confirmed. This is the sweet spot (2-3x). Large "
            f"volume on the sweep candle means real orders were executed, not "
            f"just retail noise. Backtest: vol 2-3x had WR=66.7% on best combos."
        )
    if vol >= 1.5:
        return (
            f"  Volume: {vol:.1f}x the 20-bar average — above-average volume "
            f"suggests institutional activity. Solid confirmation."
        )
    if vol >= 1.0:
        return (
            f"  Volume: {vol:.1f}x the 20-bar average — slightly above average. "
            f"Moderate confirmation only. Higher would be preferable."
        )
    return (
        f"  Volume: {vol:.1f}x the 20-bar average — below average. "
        f"Weaker confirmation — no institutional size confirmed."
    )


def _explain_delta(delta: float, direction: str) -> str:
    """
    Delta flow: net buy/sell pressure estimated from price action.
    This is the brain's closest approximation to 'buyers vs sellers'.
    Positive = net buying pressure (close near high, small upper wick).
    Negative = net selling pressure (close near low, small lower wick).
    """
    if delta is None:
        return "  Delta flow: Not available."

    if direction == 'BUY':
        if delta > 0.15:
            return (
                f"  Delta flow: +{delta:.2f} — Net BUYING pressure on this candle. "
                f"More volume was absorbed on the buy side than sell side. "
                f"This directly confirms the bullish reversal — buyers stepped "
                f"in after the sweep. (Note: this is price-action estimated delta, "
                f"not real order-book delta.)"
            )
        elif delta < -0.15:
            return (
                f"  Delta flow: {delta:.2f} — Net SELLING pressure on this candle. "
                f"This is a contra signal for a BUY trade — selling dominated "
                f"even though price swept below the swing low. Weakens conviction."
            )
        else:
            return (
                f"  Delta flow: {delta:.2f} — Neutral. Roughly equal buying and "
                f"selling pressure. Does not confirm or deny the reversal."
            )
    else:  # SELL
        if delta < -0.15:
            return (
                f"  Delta flow: {delta:.2f} — Net SELLING pressure on this candle. "
                f"Sellers are dominant after the sweep of the swing high. "
                f"Directly confirms the bearish reversal."
            )
        elif delta > 0.15:
            return (
                f"  Delta flow: +{delta:.2f} — Net BUYING pressure despite SELL signal. "
                f"Contra signal — buyers are still active. Weakens conviction."
            )
        else:
            return (
                f"  Delta flow: {delta:.2f} — Neutral. No strong directional pressure."
            )


def _explain_touches(touches: int) -> str:
    if touches == 0:
        return (
            f"  Level cleanliness: VIRGIN level — never touched before in the "
            f"lookback window. Maximum stop cluster intact. "
            f"Institutions prefer sweeping levels with fresh, untouched stop orders."
        )
    if touches == 1:
        return (
            f"  Level cleanliness: CLEAN — touched once before. "
            f"One prior test means some stops were triggered already, "
            f"but the level likely still has a significant stop cluster remaining."
        )
    if touches == 2:
        return (
            f"  Level cleanliness: TESTED — touched twice. "
            f"The level has been tested but appears to hold. Stop cluster "
            f"is partially depleted but still present."
        )
    return (
        f"  Level cleanliness: OVERUSED — touched {touches} times. "
        f"This level has been repeatedly tested. The stop cluster is likely "
        f"thinning. Historically this reduces average R but does not "
        f"eliminate edge (backtest: 3+ touches still WR=75-80% at best params)."
    )


# ═══════════════════════════════════════════════════════════════
# FACTOR LABEL EXPANDERS
# ═══════════════════════════════════════════════════════════════

def _expand_factor(factor: str) -> str:
    """Expand short confidence factor labels into plain English."""
    if factor.startswith('Vol='):
        v = factor.split('=')[1].replace('x','')
        return f"Volume {v}x avg — institutional size confirmed on sweep candle"
    if factor.startswith('Strong(') or factor.startswith('Moderate('):
        pct = factor.split('(')[1].replace(')','')
        return f"Candle closed at {pct} of range — strong directional conviction"
    if factor.startswith('Virgin'):
        return "Level is untouched — full stop cluster available for sweeping"
    if factor.startswith('Clean('):
        return "Level is clean (1 prior touch) — healthy stop cluster remaining"
    if factor.startswith('Tested('):
        return "Level tested twice — still has remaining stop orders"
    if factor.startswith('Delta='):
        v = factor.split('=')[1]
        return f"Delta flow {v} — net directional pressure confirms trade direction"
    if 'RSI' in factor and 'zone' in factor:
        v = factor.split('=')[1].split('(')[0]
        return f"RSI {v} in optimal zone — good reversal conditions"
    if factor.startswith('Mature('):
        v = factor.split('(')[1].replace('b)','')
        return f"Level is {v} bars old — mature stop cluster, well established"
    return factor


def _expand_contra(factor: str) -> str:
    """Expand short contra factor labels into plain English."""
    if 'Weak close' in factor:
        return "Weak candle close — reversal conviction is lower than ideal"
    if 'Overused' in factor:
        t = factor.split('(')[1].split('t')[0] if '(' in factor else '?'
        return f"Level touched {t} times — stop cluster may be partially depleted"
    if 'Delta neutral' in factor:
        return "No directional pressure confirmed — buyers and sellers balanced"
    if 'outside' in factor and 'zone' in factor:
        rsi_part = factor.split('=')[1].split(' ')[0] if '=' in factor else '?'
        return f"RSI {rsi_part} outside optimal range — not ideal but within allowed bounds"
    if 'Sweep' in factor and 'old' in factor:
        return "Sweep occurred on a prior candle — signal is slightly stale, momentum may have reduced"
    if 'SQUEEZE' in factor:
        return "Squeeze regime — compressed volatility reduces reversal confidence"
    return factor


# ═══════════════════════════════════════════════════════════════
# COMPACT SINGLE-LINE SUMMARY (for high-frequency logging)
# ═══════════════════════════════════════════════════════════════

def summarise_signal(
    signal:   BrainSignal,
    symbol:   str,
    regime:   str,
    bar_time: Optional[datetime] = None,
) -> str:
    """
    One-line summary for high-frequency logging. Suitable for appending to
    a CSV log or a compact terminal output. Full explain_signal() for detail.
    """
    m        = signal.measurements or {}
    time_str = bar_time.strftime('%Y-%m-%d %H:%M') if bar_time else '?'
    d        = signal.direction

    if d == 'HOLD':
        gate = m.get('decision_factor', signal.primary_evidence or 'unknown gate')
        return (
            f"[{time_str}] {symbol} HOLD — {gate} "
            f"(conf={signal.confidence:.0%} regime={regime})"
        )

    entry  = m.get('entry_price',  0)
    target = m.get('target_1',     0)
    sl     = m.get('stop_loss',    0)
    rr     = m.get('rr_achieved',  0)
    vol    = m.get('vol_ratio',    0)
    rsi    = m.get('rsi',          50)
    conf   = signal.confidence or 0.0
    touch  = m.get('touches',      0)

    factors  = " | ".join(signal.supporting_factors[:3]) if signal.supporting_factors else ""
    contras  = " | ".join(signal.contra_factors[:2])     if signal.contra_factors     else "none"

    return (
        f"[{time_str}] {symbol} {d} "
        f"entry={entry:.4g} T1={target:.4g} SL={sl:.4g} RR={rr:.1f}:1 "
        f"conf={conf:.0%} regime={regime} rsi={rsi:.0f} vol={vol:.1f}x "
        f"touches={touch} "
        f"FOR=[{factors}] AGAINST=[{contras}]"
    )


# ═══════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    from datetime import datetime

    # Simulate a HOLD signal
    hold_sig = BrainSignal(
        brain_name='Liquidity-Sweep', specialization='Stop-Hunt Reversal Detector',
        method='test', direction='HOLD', confidence=0.35, signal_strength=0.0,
        signal_age_candles=0,
        primary_evidence='Volume 0.8x < 1.5x floor required',
        supporting_factors=[], contra_factors=[],
        method_confidence=0.0, regime_suitability='LOW',
        measurements={
            'decision_factor': 'GATE_LOW_VOLUME',
            'vol_ratio': 0.8, 'rsi': 55.0,
            'price_at_signal': 172.5, 'bars_used': 250,
        },
    )
    print(explain_signal(hold_sig, 'AAPL', 'RANGING', datetime.now()))
    print()

    # Simulate a BUY signal
    buy_sig = BrainSignal(
        brain_name='Liquidity-Sweep', specialization='Stop-Hunt Reversal Detector',
        method='test', direction='BUY', confidence=0.82, signal_strength=0.17,
        signal_age_candles=0,
        primary_evidence='BULLISH_SWEEP at 172.3 (wick=0.85xATR, level_age=14b)',
        supporting_factors=['Vol=2.3x', 'Strong(88%)', 'Clean(1t)', 'RSI=62.1(BUY zone)'],
        contra_factors=['Delta neutral (0.04)'],
        method_confidence=0.85, regime_suitability='HIGH',
        measurements={
            'entry_price': 173.8, 'target_1': 177.2, 'stop_loss': 171.9,
            'swept_level': 172.3, 'wick_depth_atr': 0.85, 'close_pct': 0.88,
            'level_age': 14, 'sweep_age': 0, 'vol_ratio': 2.3, 'rsi': 62.1,
            'touches': 1, 'rr_achieved': 1.89, 'delta_flow': 0.04,
            'atr_at_signal': 1.92, 'atr_pct_at_signal': 1.10,
            'decision_factor': 'LIQUIDITY_SWEEP_BULLISH_SWEEP',
            'price_at_signal': 173.8, 'bars_used': 650,
        },
        rr_t1_mult=1.89, rr_t2_mult=2.84, rr_sl_mult=0.99,
    )
    print(explain_signal(buy_sig, 'AAPL', 'RANGING', datetime.now()))
    print()
    print("── COMPACT SUMMARY ──")
    print(summarise_signal(buy_sig, 'AAPL', 'RANGING', datetime.now()))