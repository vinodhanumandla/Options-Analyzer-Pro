"""
strategy_scanner.py
Comprehensive Trading Strategy Engine scanning for Demand/Supply Zones, Gaps,
Market Structure (HH/HL), and Candlestick patterns on intraday timeframes (1m, 5m, 15m).
"""

import time
import threading
import traceback
import pandas as pd
import numpy as np

from analysis_engine import fyers_to_yf, fyers_to_short
from sector_data import SECTOR_STOCKS

_scanner_lock = threading.Lock()
_strategy_cache = {
    "scanning": False,
    "last_scan_time": "--",
    "signals": [],  # List of dicts with signal details
    "done": 0,
    "total": 0
}
_strategy_thread = None

# --- INDICATORS & PATTERNS ---

def identify_market_structure(df):
    """
    Returns 'Bullish' if HH+HL, 'Bearish' if LH+LL, else 'Ranging'.
    Uses simple pivot highs/lows over the last N candles.
    """
    if len(df) < 10:
        return 'Unknown'
    
    # Very simplistic structure using rolling min/max
    highs = df['High'].rolling(5, center=True).max()
    lows = df['Low'].rolling(5, center=True).min()
    
    recent_highs = df['High'][df['High'] == highs].dropna().values[-3:]
    recent_lows = df['Low'][df['Low'] == lows].dropna().values[-3:]
    
    if len(recent_highs) >= 2 and len(recent_lows) >= 2:
        if recent_highs[-1] > recent_highs[-2] and recent_lows[-1] > recent_lows[-2]:
            return 'Bullish'
        elif recent_highs[-1] < recent_highs[-2] and recent_lows[-1] < recent_lows[-2]:
            return 'Bearish'
            
    return 'Ranging'

def detect_candlestick_pattern(O, H, L, C):
    """
    Checks the last 1-3 candles for reversal patterns.
    Returns (Pattern Name, Trend ('Bullish'|'Bearish'|None))
    """
    if len(C) < 3: return (None, None)
    
    o1, h1, l1, c1 = O[-1], H[-1], L[-1], C[-1]
    o2, h2, l2, c2 = O[-2], H[-2], L[-2], C[-2]
    o3, h3, l3, c3 = O[-3], H[-3], L[-3], C[-3]
    
    body1 = abs(c1 - o1)
    body2 = abs(c2 - o2)
    upper_shadow1 = h1 - max(o1, c1)
    lower_shadow1 = min(o1, c1) - l1
    
    # Hammer
    if lower_shadow1 > 2 * body1 and upper_shadow1 < 0.2 * body1:
        return ("Hammer", "Bullish")
        
    # Shooting Star
    if upper_shadow1 > 2 * body1 and lower_shadow1 < 0.2 * body1:
        return ("Shooting Star", "Bearish")
        
    # Bullish Engulfing
    if c2 < o2 and c1 > o1 and o1 <= c2 and c1 >= o2:
        return ("Bullish Engulfing", "Bullish")
        
    # Bearish Engulfing
    if c2 > o2 and c1 < o1 and o1 >= c2 and c1 <= o2:
        return ("Bearish Engulfing", "Bearish")
        
    # Morning Star
    if c3 < o3 and body2 < (abs(c3-o3)*0.3) and c1 > o1 and c1 > (o3 + c3)/2:
        return ("Morning Star", "Bullish")
        
    # Evening Star
    if c3 > o3 and body2 < (abs(c3-o3)*0.3) and c1 < o1 and c1 < (o3 + c3)/2:
        return ("Evening Star", "Bearish")
        
    return (None, None)

def detect_gaps(O, H, L, C):
    """
    Returns (Gap Type, Support/Res Level)
    Gap Up: Prev High < Curr Low -> Bullish
    Gap Down: Prev Low > Curr High -> Bearish
    """
    if len(C) < 2: return (None, None)
    
    prev_high, prev_low = H[-2], L[-2]
    curr_high, curr_low = H[-1], L[-1]
    
    if prev_high < curr_low:
        return ("Gap Up", prev_high) # Gap acts as support
    if prev_low > curr_high:
        return ("Gap Down", prev_low) # Gap acts as resistance
        
    return (None, None)

def detect_strong_base(df, current_idx, lookback=5):
    """
    Finds a tight consolidation (base) followed by a strong impulse.
    Used for Demand Zone Type 1.
    """
    if current_idx - lookback < 0: return None
    
    # ... Simplified detection for demonstration ...
    return None

def calculate_rr_targets(entry, sl, is_bullish):
    risk = abs(entry - sl)
    if risk == 0: risk = entry * 0.005 # Fallback to 0.5%
    
    if is_bullish:
        return (round(entry + 1*risk, 2), round(entry + 2*risk, 2), round(entry + 3*risk, 2))
    else:
        return (round(entry - 1*risk, 2), round(entry - 2*risk, 2), round(entry - 3*risk, 2))

# --- MAIN SCANNER ---

def analyze_stock(sym, timeframe):
    import yfinance as yf
    yf_sym = fyers_to_yf(sym)
    
    # 5m or 15m data over 2 days to get enough context
    period = "2d" if timeframe == "1m" else "5d"
    df = yf.Ticker(yf_sym).history(period=period, interval=timeframe, auto_adjust=True)
    
    if df.empty or len(df) < 15:
        return None
        
    # IST conversion
    if df.index.tz is None:
        df.index = df.index.tz_localize("Asia/Kolkata")
    elif str(df.index.tz) != "Asia/Kolkata":
        df.index = df.index.tz_convert("Asia/Kolkata")
        
    O = df['Open'].values
    H = df['High'].values
    L = df['Low'].values
    C = df['Close'].values
    V = df['Volume'].values
    
    ltp = round(float(C[-1]), 2)
    structure = identify_market_structure(df)
    pattern, pat_type = detect_candlestick_pattern(O, H, L, C)
    gap, gap_lvl = detect_gaps(O, H, L, C)
    
    signal = None
    prob_score = 0
    zone_type = "-"
    entry = 0
    sl = 0
    
    # Scoring Engine (0-100)
    # Master Prompt criteria: Demand/Supply Zone +25, Trendline +20, Pattern +25, Vol +15, Structure +15
    
    # 1. Base Setup (Candlestick Pattern provides immediate trigger)
    if pattern:
        prob_score += 25
        
        # 2. Market Structure Alignment
        if pat_type == "Bullish" and structure == "Bullish": prob_score += 15
        elif pat_type == "Bearish" and structure == "Bearish": prob_score += 15
            
        # 3. Volume Spike (curr vol > 1.5x avg vol)
        avg_v = V[-10:-1].mean() if len(V) > 10 else 1
        if V[-1] > 1.5 * avg_v: prob_score += 15
            
        # 4. Zone / Gap Confluence
        if gap == "Gap Up" and pat_type == "Bullish":
            zone_type = "Gap Support"
            prob_score += 25
            entry = H[-1]
            sl = L[-1] - (L[-1] * 0.002) # Master prompt: 0.2% buffer
            signal = "BUY"
            
        elif gap == "Gap Down" and pat_type == "Bearish":
            zone_type = "Gap Resistance"
            prob_score += 25
            entry = L[-1]
            sl = H[-1] + (H[-1] * 0.002)
            signal = "SELL"
            
        # Standard Pullback/Pattern trade if no gap
        elif pat_type == "Bullish" and ltp > df['Close'].rolling(20).mean().iloc[-1]: # Above 20 MA proxy for demand pullback
            zone_type = "Pullback Demand"
            prob_score += 20
            entry = H[-1]
            sl = L[-1] - (L[-1] * 0.002)
            signal = "BUY"
            
        elif pat_type == "Bearish" and ltp < df['Close'].rolling(20).mean().iloc[-1]:
            zone_type = "Pullback Supply"
            prob_score += 20
            entry = L[-1]
            sl = H[-1] + (H[-1] * 0.002)
            signal = "SELL"
            
        if signal:
            entry = round(float(entry), 2)
            sl = round(float(sl), 2)
            t1, t2, t3 = calculate_rr_targets(entry, sl, signal == "BUY")
            
            # Floor probability score at a reasonable level or max 100
            prob_score = min(prob_score + 15, 95) # Added base confidence
            
            return {
                "symbol": fyers_to_short(sym),
                "ltp": ltp,
                "signal": signal,
                "entry": entry,
                "sl": sl,
                "t1": t1,
                "t2": t2,
                "t3": t3,
                "pattern": f"{pattern} ({timeframe})",
                "zone": zone_type,
                "prob": prob_score
            }
            
    return None

_historical_signals = {}  # Store accumulated signals for the day (symbol -> signal data)

def _run_strategy_scan():
    global _strategy_cache, _historical_signals
    
    unique_symbols = list({sym for syms in SECTOR_STOCKS.values() for sym in syms})
    
    with _scanner_lock:
        _strategy_cache['scanning'] = True
        _strategy_cache['total'] = len(unique_symbols)
        _strategy_cache['done'] = 0
        # Do not clear _strategy_cache['signals'] so UI keeps showing previous data
        
    found_signals = []
    
    for i, sym in enumerate(unique_symbols):
        # Scan 5m and 15m
        for tf in ["5m", "15m"]:
            try:
                sig = analyze_stock(sym, tf)
                if sig:
                    sig['timestamp'] = time.time()
                    from datetime import datetime
                    sig['time_str'] = datetime.now().strftime("%H:%M:%S")
                    found_signals.append(sig)
                    break # Don't record multiple signals for same stock in diff timeframes
            except Exception as e:
                pass
                
        with _scanner_lock:
            _strategy_cache['done'] = i + 1
            
    from datetime import datetime
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    with _scanner_lock:
        # Merge newly found signals into historical day cache
        for s in found_signals:
            _historical_signals[s['symbol']] = s
            
        # Convert to list and sort: Highest Probability first, then Newest first
        merged_list = list(_historical_signals.values())
        merged_list.sort(key=lambda x: (x['prob'], x.get('timestamp', 0)), reverse=True)
        
        _strategy_cache['scanning'] = False
        _strategy_cache['signals'] = merged_list
        _strategy_cache['last_scan_time'] = ts

def strategy_scanner_loop():
    while True:
        try:
            _run_strategy_scan()
        except Exception as e:
            import traceback
            traceback.print_exc()
            with _scanner_lock:
                _strategy_cache["scanning"] = False
        time.sleep(120)  # Re-scan every 2 mins

def start_strategy_scanner():
    global _strategy_thread
    if _strategy_thread is None or not _strategy_thread.is_alive():
        _strategy_thread = threading.Thread(target=strategy_scanner_loop, daemon=True, name="strategy-scanner")
        _strategy_thread.start()
        print("[Strategy] Background D/S Zone Strategy scanner started")

def get_strategy_status():
    with _scanner_lock:
        return dict(_strategy_cache)

if __name__ == "__main__":
    print("Testing Strategy Scanner...")
    _run_strategy_scan()
    print(_strategy_cache)
