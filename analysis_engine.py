"""
Analysis Engine -- R-Factor, Sector Performance, Institutional Zones
Options Analyzer Pro

Institutional Zone logic:
  Demand Zone  = bearish origin candle + LOOKBACK consecutive bullish candles after it
  Supply Zone  = bullish origin candle + LOOKBACK consecutive bearish candles after it
  (Pine Script / institutional zone scanner logic)
"""
import threading
import time
import pandas as pd
import numpy as np
from sector_data import SECTOR_STOCKS

# ── Constants ──────────────────────────────────────────────────────────────────
LOOKBACK      = 5        # consecutive confirming candles
INTERVAL      = "5m"     # yfinance interval
PERIOD        = "5d"     # yfinance period
TOP_N         = 10       # top results per side
SCAN_EVERY_S  = 300      # re-scan every 5 minutes

# ── Cached zone scan results (updated by background thread) ───────────────────
_zone_cache = {
    "demand":    [],   # BUY SIDE top N
    "supply":    [],   # SELL SIDE top N
    "scan_time": "--",
    "scanning":  False,
    "total":     0,
    "done":      0,
}
_zone_lock   = threading.Lock()
_scan_thread = None
_scan_started = False

# ── Cached ORB scan results (updated by background thread) ────────────────────
_orb_cache = {
    "orb5m":     [],   # 5m ORB stocks
    "orb15m":    [],   # 15m ORB stocks
    "scan_time": "--",
    "scanning":  False,
    "total":     0,
    "done":      0,
}
_orb_lock     = threading.Lock()
_orb_thread   = None
_orb_started  = False


# ── Symbol conversion helpers ──────────────────────────────────────────────────
def fyers_to_short(fyers_sym):
    """'NSE:RELIANCE-EQ' -> 'RELIANCE'"""
    return fyers_sym.replace("NSE:", "").replace("-EQ", "").replace("-BE", "")


def fyers_to_yf(fyers_sym):
    """'NSE:RELIANCE-EQ' -> 'RELIANCE.NS'"""
    short = fyers_to_short(fyers_sym)
    # yfinance needs ^ for index and & encoded; handle M&M etc
    short = short.replace("&", "%26")
    return f"{short}.NS"


def get_sector_for_fyers_sym(fyers_sym):
    for sector, syms in SECTOR_STOCKS.items():
        if fyers_sym in syms:
            return sector
    return "Others"


# ── Pine Script-style zone detection ──────────────────────────────────────────
def detect_zones(df, lookback=LOOKBACK):
    """
    Given a DataFrame with Open/High/Low/Close/Volume columns,
    detect demand (BUY) and supply (SELL) institutional zones.

    Demand zone:
      - Origin candle = bearish (Close < Open)
      - Followed by `lookback` consecutive bullish candles
    Supply zone:
      - Origin candle = bullish (Close > Open)
      - Followed by `lookback` consecutive bearish candles

    Returns (demand_dict | None, supply_dict | None)
    """
    if df is None or len(df) < lookback + 10:
        return None, None

    C = df["Close"].values
    O = df["Open"].values
    H = df["High"].values
    L = df["Low"].values
    V = df["Volume"].values

    zb = lookback + 2        # offset: origin candle position from end
    n  = len(df) - 1         # index of latest candle

    if n < zb + 2:
        return None, None

    # Price shift from origin candle to most recent
    if C[n - zb] == 0:
        return None, None
    shift = abs(C[n - zb] - C[n - 2]) / C[n - zb] * 100.0

    zC, zO, zH, zL = C[n-zb], O[n-zb], H[n-zb], L[n-zb]
    avg_v = float(V[:-1].mean()) if len(V) > 1 else 1.0
    chg   = round((C[n] - C[n-1]) / C[n-1] * 100, 2) if C[n-1] != 0 else 0.0

    def pkg(ceiling, floor):
        return {
            "ceiling":   round(float(ceiling), 2),
            "floor":     round(float(floor), 2),
            "mid":       round(float((ceiling + floor) / 2), 2),
            "ltp":       round(float(C[n]), 2),
            "chg_pct":   chg,
            "move_pct":  round(float(shift), 2),
            "vol_ratio": round(float(V[n] / avg_v), 1) if avg_v > 0 else 0.0,
        }

    # ── Demand zone (bullish reversal from bearish origin) ──────────────────
    demand = None
    if zC < zO:   # origin candle is bearish
        bull_count = sum(
            1 for i in range(2, lookback + 2) if C[n - i] > O[n - i]
        )
        if bull_count == lookback:
            demand = pkg(ceiling=zO, floor=zL)   # zone: open → low of origin

    # ── Supply zone (bearish reversal from bullish origin) ──────────────────
    supply = None
    if zC > zO:   # origin candle is bullish
        bear_count = sum(
            1 for i in range(2, lookback + 2) if C[n - i] < O[n - i]
        )
        if bear_count == lookback:
            supply = pkg(ceiling=zH, floor=zO)   # zone: high → open of origin

    return demand, supply


# ── Fetch OHLCV from yfinance ──────────────────────────────────────────────────
def fetch_ohlcv_yf(fyers_sym, interval=INTERVAL, period=PERIOD):
    """Fetch OHLCV for a Fyers symbol using yfinance (NSE suffix)."""
    try:
        import yfinance as yf
        yf_sym = fyers_to_yf(fyers_sym)
        df = yf.Ticker(yf_sym).history(
            period=period, interval=interval, auto_adjust=True
        )
        if df.empty or len(df) < 20:
            return None
        return df
    except Exception:
        return None


# ── All-stock zone scanner ─────────────────────────────────────────────────────
def _run_zone_scan():
    """Background function: scan all stocks for demand/supply zones."""
    all_symbols = [sym for syms in SECTOR_STOCKS.values() for sym in syms]
    # Deduplicate
    seen = set()
    unique_symbols = []
    for s in all_symbols:
        if s not in seen:
            seen.add(s)
            unique_symbols.append(s)

    total = len(unique_symbols)

    with _zone_lock:
        _zone_cache["scanning"] = True
        _zone_cache["total"]    = total
        _zone_cache["done"]     = 0

    demand_list = []
    supply_list = []

    for i, sym in enumerate(unique_symbols):
        df = fetch_ohlcv_yf(sym)
        if df is not None:
            d, s = detect_zones(df)
            sector    = get_sector_for_fyers_sym(sym)
            short_sym = fyers_to_short(sym)
            if d:
                demand_list.append({"symbol": short_sym, "sector": sector, **d})
            if s:
                supply_list.append({"symbol": short_sym, "sector": sector, **s})

        with _zone_lock:
            _zone_cache["done"] = i + 1

    # Sort by move_pct descending (highest momentum first)
    demand_list.sort(key=lambda x: x["move_pct"], reverse=True)
    supply_list.sort(key=lambda x: x["move_pct"], reverse=True)

    from datetime import datetime
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    with _zone_lock:
        _zone_cache["demand"]    = demand_list[:TOP_N]
        _zone_cache["supply"]    = supply_list[:TOP_N]
        _zone_cache["scan_time"] = ts
        _zone_cache["scanning"]  = False

    print(f"[Zones] Scan done: {len(demand_list)} demand, {len(supply_list)} supply zones found")


def _zone_scanner_loop():
    """Runs forever in a daemon thread, scanning every SCAN_EVERY_S seconds."""
    while True:
        try:
            _run_zone_scan()
        except Exception as e:
            print(f"[Zones] Scanner error: {e}")
            with _zone_lock:
                _zone_cache["scanning"] = False
        time.sleep(SCAN_EVERY_S)


def start_zone_scanner():
    """Start the background zone scanner thread (call once at app startup)."""
    global _scan_thread, _scan_started
    if _scan_started:
        return
    _scan_started = True
    _scan_thread = threading.Thread(
        target=_zone_scanner_loop, daemon=True, name="zone-scanner"
    )
    _scan_thread.start()
    print("[Zones] Background zone scanner started")


def get_zone_scan_status():
    """Return current cached results + scanning status."""
    with _zone_lock:
        return dict(_zone_cache)


# ── ORB Scanner (Opening Range Breakout) ──────────────────────────────────────
def fetch_today_1m_yf(fyers_sym):
    """Fetch today's 1m candles using yfinance."""
    try:
        import yfinance as yf
        yf_sym = fyers_to_yf(fyers_sym)
        df = yf.Ticker(yf_sym).history(period="1d", interval="1m", auto_adjust=True)
        if df.empty or len(df) < 5:
            return None
        return df
    except Exception:
        return None


def calculate_orb(df):
    """
    Given today's 1m DataFrame, calculate the 5m and 15m ORB.
    Returns (orb5m_res, orb15m_res) where res is dict or None.
    """
    if df is None or len(df) < 5:
        return None, None

    import pandas as pd
    
    # Ensure index is localized to IST
    if df.index.tz is None:
        df.index = df.index.tz_localize("Asia/Kolkata")
    elif str(df.index.tz) != "Asia/Kolkata":
        df.index = df.index.tz_convert("Asia/Kolkata")

    # Filter for today only, just in case
    today_str = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")
    df_today = df.loc[today_str]
    
    if len(df_today) < 5:
        return None, None

    ltp = round(float(df_today["Close"].iloc[-1]), 2)
    vol = int(df_today["Volume"].iloc[-1]) if "Volume" in df_today.columns else 0

    res5m = None
    res15m = None

    # ORB 5m: 09:15 to 09:19 (inclusive = 5 candles)
    df_5m = df_today.between_time("09:15", "09:19")
    if len(df_5m) >= 4:  # Allow slight missing data, but need a decent range
        high_5m = round(float(df_5m["High"].max()), 2)
        low_5m  = round(float(df_5m["Low"].min()), 2)
        if ltp > high_5m:
            res5m = {"signal": "Bullish ORB", "high": high_5m, "low": low_5m, 'ltp': ltp, 'diff': round(((ltp - high_5m)/high_5m)*100, 2), "vol": vol}
        elif ltp < low_5m:
            res5m = {"signal": "Bearish ORB", "high": high_5m, "low": low_5m, 'ltp': ltp, 'diff': round(((ltp - low_5m)/low_5m)*100, 2), "vol": vol}

    # ORB 15m: 09:15 to 09:29 (inclusive = 15 candles)
    df_15m = df_today.between_time("09:15", "09:29")
    if len(df_15m) >= 12: # Allow slight missing data
        high_15m = round(float(df_15m["High"].max()), 2)
        low_15m  = round(float(df_15m["Low"].min()), 2)
        if ltp > high_15m:
            res15m = {"signal": "Bullish ORB", "high": high_15m, "low": low_15m, 'ltp': ltp, 'diff': round(((ltp - high_15m)/high_15m)*100, 2), "vol": vol}
        elif ltp < low_15m:
            res15m = {"signal": "Bearish ORB", "high": high_15m, "low": low_15m, 'ltp': ltp, 'diff': round(((ltp - low_15m)/low_15m)*100, 2), "vol": vol}

    return res5m, res15m


def _run_orb_scan():
    """Background function: scan all stocks for 5m and 15m ORB."""
    all_symbols = [sym for syms in SECTOR_STOCKS.values() for sym in syms]
    seen = set()
    unique_symbols = []
    for s in all_symbols:
        if s not in seen:
            seen.add(s)
            unique_symbols.append(s)

    total = len(unique_symbols)

    with _orb_lock:
        _orb_cache["scanning"] = True
        _orb_cache["total"]    = total
        _orb_cache["done"]     = 0

    orb5m_list = []
    orb15m_list = []

    for i, sym in enumerate(unique_symbols):
        df = fetch_today_1m_yf(sym)
        if df is not None:
            r5, r15 = calculate_orb(df)
            sector    = get_sector_for_fyers_sym(sym)
            short_sym = fyers_to_short(sym)
            
            if r5:
                orb5m_list.append({"symbol": short_sym, "sector": sector, **r5})
            if r15:
                orb15m_list.append({"symbol": short_sym, "sector": sector, **r15})

        with _orb_lock:
            _orb_cache["done"] = i + 1

    # Sort by diff magnitude descending
    orb5m_list.sort(key=lambda x: abs(x["diff"]), reverse=True)
    orb15m_list.sort(key=lambda x: abs(x["diff"]), reverse=True)

    from datetime import datetime
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    with _orb_lock:
        _orb_cache["orb5m"]     = orb5m_list
        _orb_cache["orb15m"]    = orb15m_list
        _orb_cache["scan_time"] = ts
        _orb_cache["scanning"]  = False

    print(f"[ORB] Scan done: {len(orb5m_list)} 5m ORB, {len(orb15m_list)} 15m ORB found")


def _orb_scanner_loop():
    while True:
        try:
            _run_orb_scan()
        except Exception as e:
            print(f"[ORB] Scanner error: {e}")
            with _orb_lock:
                _orb_cache["scanning"] = False
        time.sleep(120)  # Re-scan ORB every 2 minutes


def start_orb_scanner():
    global _orb_thread, _orb_started
    if _orb_started:
        return
    _orb_started = True
    _orb_thread = threading.Thread(
        target=_orb_scanner_loop, daemon=True, name="orb-scanner"
    )
    _orb_thread.start()
    print("[ORB] Background ORB scanner started")


def get_orb_scan_status():
    with _orb_lock:
        return dict(_orb_cache)


# ── R-Factor ──────────────────────────────────────────────────────────────────
def calculate_r_factor(symbol, quote_data):
    """
    R-Factor — 4-Component Momentum Score (NSE F&O stocks only)
    ────────────────────────────────────────────────────────────
    Combines four market signals to produce a single momentum score:

      Component 1 — Intraday Momentum (weight 45%)
        = (LTP - Open) / Open × 100
        Measures how much the stock has moved from today's open.
        Positive = buying pressure; negative = selling pressure.

      Component 2 — Gap Direction (weight 25%)
        = (Open - PrevClose) / PrevClose × 100
        Captures pre-market sentiment. A gap-up with follow-through
        adds to bullish conviction; gap-down signals weakness.

      Component 3 — Range Score (weight 20%)
        = ((LTP - Low) / (High - Low)) × 2 - 1   →  range [-1, +1]
        Scaled by today's range as % of prev close.
        +1 means price at the top of today's range (strong), -1 means bottom (weak).

      Component 4 — Volume Confirmation (weight 10%)
        = sign(Intraday%) × log1p(vol / avg_vol)
        Above-average volume on an up move adds confidence;
        high volume on a down move deepens the negative score.

    Final:
        R = 0.45 × Intraday% + 0.25 × Gap% + 0.20 × RangeScore×range_factor + 0.10 × VolScore

    Interpretation:
        High positive R  →  HIGH MOMENTUM  (Buy side candidate)
        High negative R  →  LOW MOMENTUM   (Sell / Weak side candidate)
    """
    try:
        ltp    = float(quote_data.get('lp',  quote_data.get('last_price', 0)))
        open_  = float(quote_data.get('open_price', ltp))
        high   = float(quote_data.get('high_price', ltp))
        low    = float(quote_data.get('low_price',  ltp))
        prev   = float(quote_data.get('prev_close_price', quote_data.get('close_price', ltp)))
        vol    = float(quote_data.get('volume', 1))
        avg_vol = float(quote_data.get('avg_trade_val', vol))

        if open_ == 0 or prev == 0:
            return 0.0

        # Component 1: Intraday Momentum
        intraday_pct = ((ltp - open_) / open_) * 100.0

        # Component 2: Gap Direction
        gap_pct = ((open_ - prev) / prev) * 100.0

        # Component 3: Range Score [-1, +1] scaled by range breadth
        price_range = high - low
        range_score = 0.0
        if price_range > 0:
            range_score = ((ltp - low) / price_range) * 2.0 - 1.0  # [-1, +1]
        range_factor = ((high - low) / prev) * 100.0                # range as % of prev close

        # Component 4: Volume Confirmation (directional)
        vol_ratio   = vol / avg_vol if avg_vol > 0 else 1.0
        vol_sign    = 1.0 if intraday_pct >= 0 else -1.0
        vol_score   = vol_sign * float(np.log1p(vol_ratio))

        # Weighted R-Factor
        r_factor = (
            0.45 * intraday_pct
            + 0.25 * gap_pct
            + 0.20 * range_score * range_factor
            + 0.10 * vol_score
        )
        return round(r_factor, 4)
    except Exception:
        return 0.0


# ── Sector Performance ─────────────────────────────────────────────────────────
def get_sector_performance(quotes_by_symbol):
    """
    Rich sector performance aggregation:
    - Simple equal-weighted average % change per sector
    - Advance / Decline / Unchanged counts
    - Top 3 gainers and top 3 losers within each sector
    - Sorted sector ranking (top gainer sector first)

    Returns list of dicts sorted by performance descending:
    [
        {
            "sector":      "Banking",
            "performance": 0.54,        # avg intraday % change
            "advances":    8,           # stocks > 0
            "declines":    4,           # stocks < 0
            "unchanged":   2,           # stocks == 0
            "total":       14,          # total stocks with data
            "top_gainers": [{"symbol":"HDFCBANK","pct":1.2,"ltp":1750.0}, ...],
            "top_losers":  [{"symbol":"SBIN","pct":-0.5,"ltp":820.0}, ...],
        },
        ...
    ]
    """
    results = []

    for sector, symbols in SECTOR_STOCKS.items():
        stock_perfs = []   # list of (short_sym, pct_change, ltp)

        for sym in symbols:
            q = quotes_by_symbol.get(sym)
            if not q:
                continue
            try:
                ltp  = float(q.get('lp', q.get('last_price', 0)))
                prev = float(q.get('prev_close_price', q.get('close_price', ltp)))
                if prev <= 0 or ltp <= 0:
                    continue
                pct = round(((ltp - prev) / prev) * 100, 2)
                short = fyers_to_short(sym)
                stock_perfs.append((short, pct, round(ltp, 2)))
            except Exception:
                continue

        if not stock_perfs:
            continue

        pcts      = [p for _, p, _ in stock_perfs]
        avg_pct   = round(float(np.mean(pcts)), 3)
        advances  = sum(1 for p in pcts if p > 0)
        declines  = sum(1 for p in pcts if p < 0)
        unchanged = sum(1 for p in pcts if p == 0)

        sorted_stocks = sorted(stock_perfs, key=lambda x: x[1], reverse=True)
        top_gainers   = [{"symbol": s, "pct": p, "ltp": l} for s, p, l in sorted_stocks[:3]]
        top_losers    = [{"symbol": s, "pct": p, "ltp": l} for s, p, l in sorted_stocks[-3:][::-1]]

        results.append({
            "sector":      sector,
            "performance": avg_pct,
            "advances":    advances,
            "declines":    declines,
            "unchanged":   unchanged,
            "total":       len(stock_perfs),
            "top_gainers": top_gainers,
            "top_losers":  top_losers,
        })

    results.sort(key=lambda x: x["performance"], reverse=True)
    return results


# ── R-Factor top stocks ────────────────────────────────────────────────────────
def get_r_factor_stocks(quotes_by_symbol, top_n=10):
    """
    Calculate R-Factor for all F&O stocks and return:
      - Top N HIGH MOMENTUM stocks  (buy side  — highest positive R-Factor)
      - Top N LOW  MOMENTUM stocks  (sell side — lowest/most-negative R-Factor)

    Only processes symbols that are in SECTOR_STOCKS (NSE F&O eligible).
    Returns: {'buy': [...], 'sell': [...]}
    """
    scores = []

    # Build a set of all F&O symbols for fast lookup
    fo_symbols = {sym for syms in SECTOR_STOCKS.values() for sym in syms}

    for sym, q in quotes_by_symbol.items():
        # Skip non-F&O symbols
        if sym not in fo_symbols:
            continue

        r = calculate_r_factor(sym, q)
        try:
            ltp   = float(q.get('lp', q.get('last_price', 0)))
            open_ = float(q.get('open_price', ltp))
            high  = float(q.get('high_price', ltp))
            low   = float(q.get('low_price',  ltp))
            prev  = float(q.get('prev_close_price', q.get('close_price', ltp)))
            pct   = round(((ltp - prev) / prev) * 100, 2) if prev > 0 else 0.0
        except Exception:
            ltp = open_ = high = low = prev = 0.0
            pct = 0.0

        short_sym = fyers_to_short(sym)
        sector    = get_sector_for_fyers_sym(sym)

        scores.append({
            'symbol':         short_sym,
            'sector':         sector,
            'r_factor':       r,
            'ltp':            round(ltp, 2),
            'open':           round(open_, 2),
            'high':           round(high, 2),
            'low':            round(low, 2),
            'pct_change':     pct,
        })

    # Sort descending → best momentum at top
    sorted_scores = sorted(scores, key=lambda x: x['r_factor'], reverse=True)

    # Label and slice
    buy_side  = sorted_scores[:top_n]
    sell_side = sorted_scores[-top_n:][::-1]   # reverse so weakest is first

    for s in buy_side:
        s['momentum_label'] = 'High Momentum'
    for s in sell_side:
        s['momentum_label'] = 'Low Momentum'

    return {
        'buy':  buy_side,
        'sell': sell_side,
    }


# ── Sector Stocks Detail ───────────────────────────────────────────────────────
def get_sector_stocks_data(quotes_by_symbol, sector_name):
    """Return detailed quote data for all stocks in a given sector."""
    from sector_data import SECTOR_STOCKS
    symbols = SECTOR_STOCKS.get(sector_name, [])
    results = []

    for sym in symbols:
        q = quotes_by_symbol.get(sym)
        short_sym = fyers_to_short(sym)
        if not q:
            results.append({
                'symbol': short_sym, 'ltp': 'N/A', 'open': 'N/A',
                'high': 'N/A', 'low': 'N/A', 'prev_close': 'N/A',
                'pct_change': 'N/A', 'volume': 'N/A', 'r_factor': 'N/A'
            })
            continue
        try:
            ltp   = float(q.get('lp', q.get('last_price', 0)))
            prev  = float(q.get('prev_close_price', q.get('close_price', ltp)))
            pct   = round(((ltp - prev) / prev) * 100, 2) if prev > 0 else 0.0
            rf    = calculate_r_factor(sym, q)
            vol   = int(q.get('volume', 0))

            results.append({
                'symbol':     short_sym,
                'ltp':        round(ltp, 2),
                'open':       round(float(q.get('open_price', ltp)), 2),
                'high':       round(float(q.get('high_price', ltp)), 2),
                'low':        round(float(q.get('low_price', ltp)), 2),
                'prev_close': round(prev, 2),
                'pct_change': pct,
                'volume':     f"{vol:,}",
                'r_factor':   rf
            })
        except Exception:
            results.append({'symbol': short_sym, 'ltp': 'ERR', 'pct_change': 0})

    results.sort(
        key=lambda x: float(x['pct_change']) if x['pct_change'] != 'N/A' else 0,
        reverse=True
    )
    return results
