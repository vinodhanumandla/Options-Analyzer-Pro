"""
Sector Data — NSE F&O Eligible Stocks by Sector
Options Analyzer Pro

IMPORTANT: Only stocks that are part of the NSE Futures & Options (F&O) segment
are included here. Non-F&O stocks have been excluded.
Last updated: Feb 2026 based on NSE F&O permitted list.
"""

SECTOR_STOCKS = {
    "Banking": [
        "NSE:HDFCBANK-EQ",    # Verified F&O
        "NSE:ICICIBANK-EQ",   # Verified F&O
        "NSE:SBIN-EQ",        # Verified F&O
        "NSE:KOTAKBANK-EQ",   # Verified F&O
        "NSE:AXISBANK-EQ",    # Verified F&O
        "NSE:BANKBARODA-EQ",  # Verified F&O
        "NSE:PNB-EQ",         # Verified F&O
        "NSE:CANBK-EQ",       # Verified F&O
        "NSE:INDUSINDBK-EQ",  # Verified F&O
        "NSE:FEDERALBNK-EQ",  # Verified F&O
        "NSE:IDFCFIRSTB-EQ",  # Verified F&O
        "NSE:RBLBANK-EQ",     # Verified F&O
        "NSE:BANDHANBNK-EQ",  # Verified F&O
        "NSE:AUBANK-EQ",      # Verified F&O
    ],

    "Automobile": [
        "NSE:MARUTI-EQ",      # Verified F&O
        "NSE:TATAMOTORS-EQ",  # Verified F&O
        "NSE:M&M-EQ",         # Verified F&O
        "NSE:BAJAJ-AUTO-EQ",  # Verified F&O
        "NSE:HEROMOTOCO-EQ",  # Verified F&O
        "NSE:EICHERMOT-EQ",   # Verified F&O
        "NSE:TVSMOTOR-EQ",    # Verified F&O
        "NSE:ASHOKLEY-EQ",    # Verified F&O
        "NSE:MOTHERSON-EQ",   # Verified F&O
        "NSE:BOSCHLTD-EQ",    # Verified F&O
        "NSE:EXIDEIND-EQ",    # Verified F&O
        "NSE:TIINDIA-EQ",     # Verified F&O
        "NSE:APOLLOTYRE-EQ",  # Verified F&O
        "NSE:BALKRISIND-EQ",  # Verified F&O
        "NSE:BHARATFORG-EQ",  # Verified F&O
    ],

    "Pharma": [
        "NSE:SUNPHARMA-EQ",   # Verified F&O
        "NSE:DRREDDY-EQ",     # Verified F&O
        "NSE:CIPLA-EQ",       # Verified F&O
        "NSE:DIVISLAB-EQ",    # Verified F&O
        "NSE:BIOCON-EQ",      # Verified F&O
        "NSE:LUPIN-EQ",       # Verified F&O
        "NSE:AUROPHARMA-EQ",  # Verified F&O
        "NSE:ALKEM-EQ",       # Verified F&O
        "NSE:TORNTPHARM-EQ",  # Verified F&O
        "NSE:GLENMARK-EQ",    # Verified F&O
        "NSE:IPCALAB-EQ",     # Verified F&O
        "NSE:LAURUSLABS-EQ",  # Verified F&O
        "NSE:GRANULES-EQ",    # Verified F&O
        "NSE:NATCOPHARM-EQ",  # Verified F&O
    ],

    "Financial Services": [
        "NSE:BAJFINANCE-EQ",  # Verified F&O
        "NSE:BAJAJFINSV-EQ",  # Verified F&O
        "NSE:HDFCLIFE-EQ",    # Verified F&O
        "NSE:SBILIFE-EQ",     # Verified F&O
        "NSE:ICICIGI-EQ",     # Verified F&O
        "NSE:HDFCAMC-EQ",     # Verified F&O
        "NSE:MUTHOOTFIN-EQ",  # Verified F&O
        "NSE:CHOLAFIN-EQ",    # Verified F&O
        "NSE:M&MFIN-EQ",      # Verified F&O
        "NSE:LICHSGFIN-EQ",   # Verified F&O
        "NSE:MANAPPURAM-EQ",  # Verified F&O
        "NSE:RECLTD-EQ",      # Verified F&O
        "NSE:PFC-EQ",         # Verified F&O
        "NSE:IRFC-EQ",        # Verified F&O
        "NSE:CANFINHOME-EQ",  # Verified F&O
    ],

    "FMCG": [
        "NSE:HINDUNILVR-EQ",  # Verified F&O
        "NSE:ITC-EQ",         # Verified F&O
        "NSE:NESTLEIND-EQ",   # Verified F&O
        "NSE:BRITANNIA-EQ",   # Verified F&O
        "NSE:DABUR-EQ",       # Verified F&O
        "NSE:MARICO-EQ",      # Verified F&O
        "NSE:GODREJCP-EQ",    # Verified F&O
        "NSE:COLPAL-EQ",      # Verified F&O
        "NSE:EMAMILTD-EQ",    # Verified F&O
        "NSE:TATACONSUM-EQ",  # Verified F&O
        "NSE:UBL-EQ",         # Verified F&O
        "NSE:RADICO-EQ",      # Verified F&O
        "NSE:VBL-EQ",         # Verified F&O
    ],

    "Metal & Mining": [
        "NSE:TATASTEEL-EQ",   # Verified F&O
        "NSE:JSWSTEEL-EQ",    # Verified F&O
        "NSE:HINDALCO-EQ",    # Verified F&O
        "NSE:VEDL-EQ",        # Verified F&O
        "NSE:SAIL-EQ",        # Verified F&O
        "NSE:NATIONALUM-EQ",  # Verified F&O
        "NSE:NMDC-EQ",        # Verified F&O
        "NSE:COALINDIA-EQ",   # Verified F&O
        "NSE:HINDZINC-EQ",    # Verified F&O
        "NSE:JINDALSTEL-EQ",  # Verified F&O
        "NSE:APLAPOLLO-EQ",   # Verified F&O
    ],

    "Power & Utilities": [
        "NSE:NTPC-EQ",        # Verified F&O
        "NSE:POWERGRID-EQ",   # Verified F&O
        "NSE:ADANIPOWER-EQ",  # Verified F&O
        "NSE:TATAPOWER-EQ",   # Verified F&O
        "NSE:CESC-EQ",        # Verified F&O
        "NSE:TORNTPOWER-EQ",  # Verified F&O
        "NSE:JSWENERGY-EQ",   # Verified F&O
        "NSE:ADANIGREEN-EQ",  # Verified F&O
        "NSE:IEX-EQ",         # Verified F&O
        "NSE:NHPC-EQ",        # Verified F&O
        "NSE:SJVN-EQ",        # Verified F&O
    ],

    "Oil & Gas": [
        "NSE:RELIANCE-EQ",    # Verified F&O
        "NSE:ONGC-EQ",        # Verified F&O
        "NSE:IOC-EQ",         # Verified F&O
        "NSE:BPCL-EQ",        # Verified F&O
        "NSE:HINDPETRO-EQ",   # Verified F&O
        "NSE:GAIL-EQ",        # Verified F&O
        "NSE:MGL-EQ",         # Verified F&O
        "NSE:IGL-EQ",         # Verified F&O
        "NSE:PETRONET-EQ",    # Verified F&O
        "NSE:GUJGASLTD-EQ",   # Verified F&O
    ],

    "Capital Goods": [
        "NSE:LT-EQ",          # Verified F&O
        "NSE:SIEMENS-EQ",     # Verified F&O
        "NSE:ABB-EQ",         # Verified F&O
        "NSE:HAVELLS-EQ",     # Verified F&O
        "NSE:BHEL-EQ",        # Verified F&O
        "NSE:CGPOWER-EQ",     # Verified F&O
        "NSE:VOLTAS-EQ",      # Verified F&O
        "NSE:THERMAX-EQ",     # Verified F&O
        "NSE:CUMMINSIND-EQ",  # Verified F&O
        "NSE:BEL-EQ",         # Verified F&O
        "NSE:AIAENG-EQ",      # Verified F&O
        "NSE:POLYCAB-EQ",     # Verified F&O
        "NSE:KEI-EQ",         # Verified F&O
    ],

    "Infra": [
        "NSE:ADANIPORTS-EQ",  # Verified F&O
        "NSE:ULTRACEMCO-EQ",  # Verified F&O
        "NSE:ACC-EQ",         # Verified F&O
        "NSE:AMBUJACEM-EQ",   # Verified F&O
        "NSE:JKCEMENT-EQ",    # Verified F&O
        "NSE:RAMCOCEM-EQ",    # Verified F&O
        "NSE:DALBHARAT-EQ",   # Verified F&O
        "NSE:GMRAIRPORT-EQ",  # Verified F&O
        "NSE:IRB-EQ",         # Verified F&O
        "NSE:KNRCON-EQ",      # Verified F&O
        "NSE:PNCINFRA-EQ",    # Verified F&O
        "NSE:NCC-EQ",         # Verified F&O
        "NSE:IRCTC-EQ",       # Verified F&O
    ],

    "Technology": [
        "NSE:TCS-EQ",         # Verified F&O
        "NSE:INFY-EQ",        # Verified F&O
        "NSE:WIPRO-EQ",       # Verified F&O
        "NSE:HCLTECH-EQ",     # Verified F&O
        "NSE:TECHM-EQ",       # Verified F&O
        "NSE:LTIM-EQ",        # Verified F&O
        "NSE:MPHASIS-EQ",     # Verified F&O
        "NSE:COFORGE-EQ",     # Verified F&O
        "NSE:PERSISTENT-EQ",  # Verified F&O
        "NSE:OFSS-EQ",        # Verified F&O
        "NSE:KPITTECH-EQ",    # Verified F&O
        "NSE:TATAELXSI-EQ",   # Verified F&O
        "NSE:CYIENT-EQ",      # Verified F&O
        "NSE:ZENSAR-EQ",      # Verified F&O
    ],

    "Defence": [
        "NSE:HAL-EQ",         # Verified F&O
        "NSE:BEL-EQ",         # Verified F&O
        "NSE:BHEL-EQ",        # Verified F&O
        "NSE:MAZDOCK-EQ",     # Verified F&O
        "NSE:COCHINSHIP-EQ",  # Verified F&O
        "NSE:GRSE-EQ",        # Verified F&O
        "NSE:BEML-EQ",        # Verified F&O
    ],

    "Industrial": [
        "NSE:ASTRAL-EQ",      # Verified F&O
        "NSE:SUPREMEIND-EQ",  # Verified F&O
        "NSE:PIDILITIND-EQ",  # Verified F&O
        "NSE:KAJARIACER-EQ",  # Verified F&O
        "NSE:POLYCAB-EQ",     # Verified F&O
        "NSE:KEI-EQ",         # Verified F&O
        "NSE:SCHAEFFLER-EQ",  # Verified F&O
        "NSE:SKF-EQ",         # Verified F&O
        "NSE:TIMKEN-EQ",      # Verified F&O
    ],

    "Logistics": [
        "NSE:CONCOR-EQ",      # Verified F&O
        "NSE:DELHIVERY-EQ",   # Verified F&O
    ],

    "Diversified": [
        "NSE:ADANIENT-EQ",    # Verified F&O
        "NSE:ITC-EQ",         # Verified F&O
        "NSE:TATACOMM-EQ",    # Verified F&O
    ],

    "Textiles": [
        "NSE:PAGEIND-EQ",     # Verified F&O
        "NSE:RAYMOND-EQ",     # Verified F&O
        "NSE:TRIDENT-EQ",     # Verified F&O
        "NSE:GRASIM-EQ",      # Verified F&O
    ],

    "Chemical": [
        "NSE:PIDILITIND-EQ",  # Verified F&O
        "NSE:SRF-EQ",         # Verified F&O
        "NSE:NAVINFLUOR-EQ",  # Verified F&O
        "NSE:DEEPAKNTR-EQ",   # Verified F&O
        "NSE:TATACHEM-EQ",    # Verified F&O
    ],
}

# Flat list of all symbols (deduplicated, preserving order)
ALL_SYMBOLS = list(dict.fromkeys(
    sym for stocks in SECTOR_STOCKS.values() for sym in stocks
))


def get_sector_for_symbol(symbol):
    """Return the sector name for a given stock symbol."""
    for sector, stocks in SECTOR_STOCKS.items():
        if symbol in stocks:
            return sector
    return "Unknown"


def get_symbols_for_sector(sector_name):
    """Return all stock symbols for the given sector."""
    return SECTOR_STOCKS.get(sector_name, [])


def get_all_sectors():
    """Return list of all sector names."""
    return list(SECTOR_STOCKS.keys())
