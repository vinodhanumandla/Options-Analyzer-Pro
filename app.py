"""
Options Analyzer Pro -- Flask Main Application
"""
import sys, os, json
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from dotenv import load_dotenv

load_dotenv()

# Force UTF-8 output so Unicode in print() never crashes on Windows CP1252 terminal
# Must reconfigure BOTH stdout AND stderr
for _stream_name in ('stdout', 'stderr'):
    _stream = getattr(sys, _stream_name, None)
    if _stream and hasattr(_stream, 'reconfigure'):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

# Also set env var so child processes (werkzeug reloader) inherit UTF-8
os.environ["PYTHONIOENCODING"] = "utf-8"

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "options-analyzer-pro-secret-2024")

# Global error handler: always return JSON (never HTML 500) for API routes
@app.errorhandler(Exception)
def handle_exception(e):
    # Safely log — never let logging itself crash the app
    try:
        import traceback, datetime
        tb_str = traceback.format_exc()
        try:
            print(f"[Error] {request.path}: {e}")
        except Exception:
            pass  # Ignore print errors
            
        # Write to a file instead of relying on stderr
        with open("flask_error_log.txt", "a", encoding="utf-8") as f:
            f.write(f"\n--- {datetime.datetime.now()} ---\n{request.path}\n{tb_str}\n")
    except Exception:
        tb_str = "Error formatting traceback"

    # For API routes always return JSON
    if request.path.startswith("/api/"):
        err_msg = str(e).encode('ascii', errors='replace').decode('ascii')
        return jsonify({"success": False, "error": err_msg, "traceback": tb_str[-1000:]}), 500
    # For navigation routes let Flask show its default page
    raise e






# In-memory store for Fyers connection (also backed by Firebase if configured)
_fyers_instance = None

# ─── Firebase Init (optional) ─────────────────────────────────────────────────
try:
    from firebase_config import init_firebase, save_credentials, load_credentials, clear_credentials
    FIREBASE_URL = os.environ.get("FIREBASE_DB_URL", "")
    _firebase_ok = init_firebase(FIREBASE_URL)
except Exception as _fe:
    print(f"[App] Firebase not available: {_fe}")
    _firebase_ok = False

    def save_credentials(*a, **kw): pass
    def load_credentials(): return None
    def clear_credentials(): pass

# ─── Background Scanners Startup ───────────────────────────────────────────────
# Start scanners immediately at app import time
try:
    from analysis_engine import start_zone_scanner, start_orb_scanner
    from strategy_scanner import start_strategy_scanner
    start_zone_scanner()
    start_orb_scanner()
    start_strategy_scanner()
except Exception as _ze:
    print(f"[App] Scanner startup error: {_ze}")

# ─── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Landing page — Fyers credential input."""
    connected = session.get("connected", False)
    if connected:
        return redirect(url_for("dashboard"))
    return render_template("index.html")


@app.route("/generate-token-url", methods=["POST"])
def generate_token_url():
    """Return the Fyers OAuth URL for the frontend to open."""
    from fyers_auth import generate_auth_url
    data       = request.get_json()
    app_id     = data.get("app_id", "").strip()
    secret_key = data.get("secret_key", "").strip()
    redirect_uri = data.get("redirect_uri", "https://trade.fyers.in/api-login/redirect-uri/index.html")

    if not app_id or not secret_key:
        return jsonify({"success": False, "error": "App ID and Secret Key are required."})

    # Store in session for later token exchange
    session["app_id"]     = app_id
    session["secret_key"] = secret_key

    auth_url = generate_auth_url(app_id, redirect_uri)
    return jsonify({"success": True, "auth_url": auth_url})


@app.route("/connect", methods=["POST"])
def connect():
    """
    Connect to Fyers — fast path with hard timeouts.

    Strategy:
    • If user pasted full redirect URL → extract auth_code → exchange (10s timeout)
    • If user pasted bare token (no URL) → use directly, skip slow exchange
    • get_profile() runs with 5s timeout (non-fatal — only used for display name)
    • Firebase save runs in background thread (never blocks response)
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    global _fyers_instance

    from fyers_auth import extract_auth_code, exchange_auth_code

    data       = request.get_json()
    raw_input  = data.get("access_token", "").strip()
    app_id     = data.get("app_id",     session.get("app_id",     "")).strip()
    secret_key = data.get("secret_key", session.get("secret_key", "")).strip()

    if not raw_input or not app_id:
        return jsonify({"success": False, "error": "Auth code and App ID are required."})

    # ── Step 1: Determine access_token ─────────────────────────────────────────
    auth_code    = extract_auth_code(raw_input)
    is_url_input = raw_input.strip().startswith("http")
    access_token = None
    user_name    = "Trader"

    if is_url_input and secret_key:
        # User pasted redirect URL → exchange auth_code → access_token (10s max)
        print("[Connect] URL detected -> exchanging auth_code (10s timeout)...")
        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(exchange_auth_code, auth_code, app_id, secret_key)
                result = future.result(timeout=10)

            if result["success"]:
                access_token = result["access_token"]
                print("[Connect] OK: auth_code exchanged successfully")
            else:
                print(f"[Connect] Exchange failed: {result['error']} - using as direct token")
                access_token = auth_code
        except FuturesTimeout:
            print("[Connect] WARN: Exchange timed out after 10s - using raw value as token")
            access_token = auth_code
        except Exception as e:
            print(f"[Connect] Exchange error: {e} - using raw value as token")
            access_token = auth_code
    else:
        # Bare token pasted → use directly, skip exchange entirely
        print("[Connect] Bare token detected -> using directly (skip exchange)")
        access_token = auth_code

    # ── Step 2: Build FyersModel ───────────────────────────────────────────────
    try:
        from fyers_apiv3 import fyersModel
        fyers = fyersModel.FyersModel(
            client_id=app_id,
            token=access_token,
            log_path=""
        )
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not create Fyers session: {e}"})

    # ── Step 3: get_profile() with 5s timeout (non-fatal) ─────────────────────
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(fyers.get_profile)
            profile = fut.result(timeout=5)
        print(f"[Connect] Profile: {profile}")
        if profile.get("s") == "ok" or profile.get("code") == 200:
            d = profile.get("data", {})
            user_name = d.get("name") or d.get("fy_id") or "Trader"
    except FuturesTimeout:
        print("[Connect] WARN: get_profile timed out (5s) - proceeding without name")
    except Exception as pe:
        print(f"[Connect] get_profile error (non-fatal): {pe}")

    # ── Mark connected ─────────────────────────────────────────────────────────
    _fyers_instance          = fyers
    session["connected"]     = True
    session["access_token"]  = access_token
    session["app_id"]        = app_id
    session["user_name"]     = user_name

    # Firebase save in background (never blocks response)
    threading.Thread(
        target=save_credentials, args=(app_id, access_token), daemon=True
    ).start()

    print(f"[Connect] OK: Connected as '{user_name}'")
    return jsonify({"success": True, "name": user_name})






@app.route("/disconnect", methods=["POST"])
def disconnect():
    """Disconnect from Fyers. Session cleared immediately; Firebase cleanup is non-blocking."""
    global _fyers_instance
    _fyers_instance = None
    session.clear()

    # Firebase cleanup in background so a slow/failing Firebase never blocks the user
    import threading
    threading.Thread(target=clear_credentials, daemon=True).start()

    return jsonify({"success": True})




@app.route("/dashboard")
def dashboard():
    """Main dashboard (requires connection)."""
    if not session.get("connected"):
        return redirect(url_for("index"))
    user_name = session.get("user_name", "Trader")
    return render_template("dashboard.html", user_name=user_name)


# ─── API Endpoints ────────────────────────────────────────────────────────────

def _get_quotes(symbols):
    """Helper: fetch live quotes from Fyers (connected) or fall back to sample data."""
    from fyers_auth import fetch_quotes, get_sample_quotes
    global _fyers_instance

    if _fyers_instance:
        try:
            data = fetch_quotes(_fyers_instance, symbols)
            if data:
                return data
        except Exception as e:
            print(f"[App] Live quote fetch error: {e}")
    print("[App] WARNING: Using SAMPLE (mock) data — not connected to Fyers live.")
    return get_sample_quotes(symbols)


@app.route("/api/status", methods=["GET"])
def api_status():
    """Return connection status and data source mode (live vs sample)."""
    global _fyers_instance
    connected  = bool(_fyers_instance and session.get("connected"))
    return jsonify({
        "connected":   connected,
        "data_source": "LIVE" if connected else "SAMPLE",
        "user":        session.get("user_name", "—"),
    })



@app.route("/api/debug-quote", methods=["GET"])
def api_debug_quote():
    """Debug: return raw Fyers quote response for one symbol to confirm field names."""
    global _fyers_instance
    if not _fyers_instance:
        return jsonify({"error": "not connected"})
    try:
        from sector_data import ALL_SYMBOLS
        sym = ALL_SYMBOLS[0] if ALL_SYMBOLS else "NSE:RELIANCE-EQ"
        raw = _fyers_instance.quotes(data={"symbols": sym})
        return jsonify({"raw": raw})
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/sector-performance", methods=["GET"])
def api_sector_performance():
    """Sector performance bar chart data."""
    if not session.get("connected"):
        return jsonify({"success": False, "error": "Not connected"})

    from analysis_engine import get_sector_performance
    from sector_data import ALL_SYMBOLS

    quotes = _get_quotes(ALL_SYMBOLS)
    data   = get_sector_performance(quotes)   # now returns sorted list of dicts

    return jsonify({"success": True, "data": data})



@app.route("/api/r-factor", methods=["GET"])
def api_r_factor():
    """Top 10 buy and sell momentum stocks."""
    if not session.get("connected"):
        return jsonify({"success": False, "error": "Not connected"})

    from analysis_engine import get_r_factor_stocks
    from sector_data import ALL_SYMBOLS

    quotes = _get_quotes(ALL_SYMBOLS)
    result = get_r_factor_stocks(quotes, top_n=10)

    return jsonify({"success": True, "data": result})


@app.route("/api/institutional-zones", methods=["GET"])
def api_institutional_zones():
    """
    Return cached institutional zone scan results.
    demand = BUY SIDE top 10 (bearish origin + bullish confirmation)
    supply = SELL SIDE top 10 (bullish origin + bearish confirmation)
    """
    if not session.get("connected"):
        return jsonify({"success": False, "error": "Not connected"})

    from analysis_engine import get_zone_scan_status
    cache = get_zone_scan_status()

    return jsonify({
        "success":   True,
        "demand":    cache["demand"],
        "supply":    cache["supply"],
        "scan_time": cache["scan_time"],
        "scanning":  cache["scanning"],
        "progress":  {
            "done":  cache["done"],
            "total": cache["total"],
        }
    })


@app.route("/api/institutional-zones/rescan", methods=["POST"])
def api_zones_rescan():
    """Trigger an immediate zone re-scan in background."""
    if not session.get("connected"):
        return jsonify({"success": False, "error": "Not connected"})
    import threading
    from analysis_engine import _run_zone_scan
    threading.Thread(target=_run_zone_scan, daemon=True).start()
    return jsonify({"success": True, "message": "Re-scan started"})


@app.route("/api/strategy-scanner", methods=["GET"])
def api_strategy_scanner():
    """Return cached D/S Zone Strategy scan results."""
    try:
        from strategy_scanner import get_strategy_status
        cache = get_strategy_status()

        # Sanitise numpy/non-serialisable types
        def _safe(obj):
            import numpy as np
            if isinstance(obj, (np.integer,)):     return int(obj)
            if isinstance(obj, (np.floating,)):    return float(obj)
            if isinstance(obj, (np.ndarray,)):     return obj.tolist()
            if isinstance(obj, list):              return [_safe(i) for i in obj]
            if isinstance(obj, dict):              return {k: _safe(v) for k, v in obj.items()}
            return obj

        return jsonify({
            "success":   True,
            "signals":   _safe(cache["signals"]),
            "scan_time": cache["last_scan_time"],
            "scanning":  bool(cache["scanning"]),
            "progress":  {
                "done":  int(cache.get("done", 0)),
                "total": int(cache.get("total", 0)),
            }
        })
    except Exception as e:
        import traceback
        tb_str = traceback.format_exc()
        err_msg = str(e).encode('ascii', errors='replace').decode('ascii')
        return jsonify({"success": False, "error": err_msg, "traceback": tb_str[-1000:]}), 500


@app.route("/api/strategy-scanner/rescan", methods=["POST"])
def api_strategy_rescan():
    """Trigger an immediate strategy re-scan in background."""
    if not session.get("connected"):
        return jsonify({"success": False, "error": "Not connected"})
    import threading
    from strategy_scanner import _run_strategy_scan
    threading.Thread(target=_run_strategy_scan, daemon=True).start()
    return jsonify({"success": True, "message": "Strategy re-scan started"})


@app.route("/api/orb", methods=["GET"])
def api_orb():
    """
    Return cached ORB (Opening Range Breakout) scan results.
    The cache is always available (returns empty lists if scan hasn't run yet).
    """
    try:
        from analysis_engine import get_orb_scan_status
        import json as _json

        cache = get_orb_scan_status()

        # Sanitise numpy/non-serialisable types
        def _safe(obj):
            import numpy as np
            if isinstance(obj, (np.integer,)):     return int(obj)
            if isinstance(obj, (np.floating,)):    return float(obj)
            if isinstance(obj, (np.ndarray,)):     return obj.tolist()
            if isinstance(obj, list):              return [_safe(i) for i in obj]
            if isinstance(obj, dict):              return {k: _safe(v) for k, v in obj.items()}
            return obj

        return jsonify({
            "success":   True,
            "orb5m":     _safe(cache["orb5m"]),
            "orb15m":    _safe(cache["orb15m"]),
            "scan_time": cache["scan_time"],
            "scanning":  bool(cache["scanning"]),
            "progress":  {
                "done":  int(cache["done"]),
                "total": int(cache["total"]),
            }
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/heatmap", methods=["GET"])
def api_heatmap():
    """
    Stock heatmap data: all stocks grouped by sector with LTP and % change.
    Frontend renders colored tiles sized by sector, colored by % change intensity.
    """
    if not session.get("connected"):
        return jsonify({"success": False, "error": "Not connected"})

    from analysis_engine import fyers_to_short
    from sector_data import SECTOR_STOCKS

    all_symbols = [sym for syms in SECTOR_STOCKS.values() for sym in syms]
    quotes = _get_quotes(all_symbols)

    sectors = []
    for sector, symbols in SECTOR_STOCKS.items():
        stocks = []
        for sym in symbols:
            q = quotes.get(sym)
            short = fyers_to_short(sym)
            if not q:
                stocks.append({"symbol": short, "ltp": 0, "pct": 0})
                continue
            try:
                ltp  = float(q.get("lp", q.get("last_price", 0)))
                prev = float(q.get("prev_close_price", q.get("close_price", ltp)))
                pct  = round(((ltp - prev) / prev) * 100, 2) if prev > 0 else 0.0
                stocks.append({"symbol": short, "ltp": round(ltp, 2), "pct": pct})
            except Exception:
                stocks.append({"symbol": short, "ltp": 0, "pct": 0})

        avg_pct = round(sum(s["pct"] for s in stocks) / len(stocks), 3) if stocks else 0
        sectors.append({"sector": sector, "avg_pct": avg_pct, "stocks": stocks})

    # Sort sectors by avg_pct (best first)
    sectors.sort(key=lambda x: x["avg_pct"], reverse=True)
    return jsonify({"success": True, "sectors": sectors})


# ─── WSGI Error Logging Middleware (to catch elusive Windows bugs) ───────────
class DebugMiddleware:
    def __init__(self, app):
        self.app = app
    def __call__(self, environ, start_response):
        try:
            return self.app(environ, start_response)
        except Exception as e:
            import traceback
            with open("flask_crash.txt", "w") as f:
                f.write(traceback.format_exc())
            raise

app.wsgi_app = DebugMiddleware(app.wsgi_app)


@app.route("/api/sector-stocks/<sector_name>", methods=["GET"])
def api_sector_stocks(sector_name):
    """Return all stocks for a given sector with live data."""
    if not session.get("connected"):
        return jsonify({"success": False, "error": "Not connected"})

    from analysis_engine import get_sector_stocks_data
    from sector_data import SECTOR_STOCKS

    # Decode URL encoding
    sector_name = sector_name.replace("-", " ").replace("%20", " ")

    if sector_name not in SECTOR_STOCKS:
        # Try case-insensitive match
        for s in SECTOR_STOCKS:
            if s.lower() == sector_name.lower():
                sector_name = s
                break
        else:
            return jsonify({"success": False, "error": f"Sector '{sector_name}' not found"})

    symbols = SECTOR_STOCKS[sector_name]
    quotes  = _get_quotes(symbols)
    result  = get_sector_stocks_data(quotes, sector_name)

    return jsonify({"success": True, "sector": sector_name, "data": result})


if __name__ == "__main__":
    import webbrowser, threading
    print("=" * 55)
    print("  Options Analyzer Pro - Starting...")
    print("  Open browser: http://localhost:5000")
    print("=" * 55)
    # Open browser automatically after 1.5s (gives Flask time to start)
    threading.Timer(1.5, lambda: webbrowser.open_new_tab("http://localhost:5000")).start()
    app.run(debug=False, host="0.0.0.0", port=5000, use_reloader=False)


