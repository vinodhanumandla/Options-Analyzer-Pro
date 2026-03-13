"""
Fyers API Authentication Module — Options Analyzer Pro
Fyers API v3 Flow:
  1. User logs in via OAuth URL → redirected to redirect_uri?s=ok&auth_code=XXXX
  2. auth_code is exchanged server-side for access_token via SessionModel.generate_token()
  3. access_token is used to create FyersModel for all subsequent API calls
"""
import urllib.parse
import re


REDIRECT_URI = "https://trade.fyers.in/api-login/redirect-uri/index.html"


def generate_auth_url(app_id, redirect_uri=None, state="state"):
    """Generate the Fyers OAuth2 authorization URL."""
    uri = redirect_uri or REDIRECT_URI
    auth_url = (
        f"https://api-t1.fyers.in/api/v3/generate-authcode"
        f"?client_id={app_id}"
        f"&redirect_uri={urllib.parse.quote(uri)}"
        f"&response_type=code"
        f"&state={state}"
    )
    return auth_url


def extract_auth_code(user_input):
    """
    Smart parser: user pastes either:
      - Full redirect URL:  https://trade.fyers.in/...?s=ok&auth_code=eyXXX&code=eyXXX
      - Just the auth_code: eyJhbGciOiJ...
    Returns the auth_code string.
    """
    user_input = user_input.strip()

    # If it looks like a URL, extract auth_code or code param
    if user_input.startswith("http"):
        parsed = urllib.parse.urlparse(user_input)
        params = urllib.parse.parse_qs(parsed.query)
        code = (
            params.get("auth_code", [None])[0]
            or params.get("code", [None])[0]
        )
        if code:
            return code.strip()

    # Otherwise treat entire input as auth_code / access_token
    return user_input


def exchange_auth_code(auth_code, app_id, secret_key):
    """
    Exchange auth_code → access_token using Fyers SessionModel.
    Returns {"success": True, "access_token": "..."} or {"success": False, "error": "..."}
    """
    try:
        from fyers_apiv3 import fyersModel
        session = fyersModel.SessionModel(
            client_id=app_id,
            secret_key=secret_key,
            redirect_uri=REDIRECT_URI,
            response_type="code",
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        resp = session.generate_token()
        print(f"[Fyers] Token exchange response: {resp}")

        if resp.get("s") == "ok":
            return {"success": True, "access_token": resp["access_token"]}

        msg = resp.get("message") or resp.get("msg") or str(resp)
        return {"success": False, "error": msg}
    except Exception as e:
        return {"success": False, "error": str(e)}


def validate_and_connect(app_id, access_token):
    """
    Create FyersModel and validate by fetching user profile.
    Returns (fyers_instance, profile_dict) or (None, error_str).

    Fyers v3 FyersModel:
        client_id = App ID  (e.g. "XY1234-100")
        token     = access_token (the JWT returned by generate_token())
    """
    try:
        from fyers_apiv3 import fyersModel

        fyers = fyersModel.FyersModel(
            client_id=app_id,
            token=access_token,
            log_path=""
        )
        profile = fyers.get_profile()
        print(f"[Fyers] Profile response: {profile}")

        # Fyers v3 returns s=="ok" on success
        if profile.get("s") == "ok":
            data = profile.get("data", {})
            name = data.get("name") or data.get("fy_id") or "Fyers User"
            return fyers, {"name": name, "success": True}

        # Some builds return code==200
        if profile.get("code") == 200:
            data = profile.get("data", {})
            name = data.get("name") or data.get("fy_id") or "Fyers User"
            return fyers, {"name": name, "success": True}

        err = profile.get("message") or profile.get("msg") or str(profile)
        return None, err

    except Exception as e:
        return None, str(e)


def normalize_quote(v):
    """
    Normalize a Fyers API v3 quote dict ('v' sub-object) to consistent field names.

    Fyers v3 actual field names in 'v':
        lp                 → Last Traded Price  (already correct)
        open_price         → Open               (already correct)
        high_price         → High               (already correct)
        low_price          → Low                (already correct)
        prev_close_price   → Previous Close     (already correct)
        vol_traded_today   → Volume  ← Fyers v3 uses this; sample uses 'volume'
        avg_trade_val      → Avg trade value    (already correct)
        ch                 → Change (absolute)
        chp                → Change %

    We add aliases so callers can use 'volume' without caring about the 
    underlying name.
    """
    if not v:
        return v
    out = dict(v)
    # Alias vol_traded_today → volume
    if "vol_traded_today" in out and "volume" not in out:
        out["volume"] = out["vol_traded_today"]
    # Alias last_price → lp
    if "last_price" in out and "lp" not in out:
        out["lp"] = out["last_price"]
    # Alias close_price → prev_close_price
    if "close_price" in out and "prev_close_price" not in out:
        out["prev_close_price"] = out["close_price"]
    return out


def fetch_quotes(fyers, symbols):
    """
    Fetch live quotes for a list of Fyers symbols.
    Returns dict: {symbol: normalized_quote_data}
    """
    if not symbols:
        return {}

    all_quotes = {}
    for i in range(0, len(symbols), 50):
        batch = symbols[i:i + 50]
        try:
            resp = fyers.quotes(data={"symbols": ",".join(batch)})
            if resp.get("s") == "ok" or resp.get("code") == 200:
                for q in resp.get("d", []):
                    v   = q.get("v", q)
                    sym = q.get("n", "")
                    if sym:
                        all_quotes[sym] = normalize_quote(v)
        except Exception as e:
            print(f"[Fyers] Quote error batch {i}: {e}")
    return all_quotes



def get_sample_quotes(symbols):
    """Realistic mock data for UI testing without live connection."""
    import random

    quotes = {}
    for sym in symbols:
        base  = random.uniform(100, 3000)
        prev  = base * random.uniform(0.92, 1.08)
        open_ = prev  * random.uniform(0.98, 1.02)
        ltp   = open_ * random.uniform(0.97, 1.03)
        high  = max(open_, ltp) * random.uniform(1.00, 1.02)
        low   = min(open_, ltp) * random.uniform(0.98, 1.00)

        quotes[sym] = {
            "lp":               round(ltp,   2),
            "open_price":       round(open_, 2),
            "high_price":       round(high,  2),
            "low_price":        round(low,   2),
            "prev_close_price": round(prev,  2),
            "volume":           random.randint(100_000, 5_000_000),
            "avg_trade_val":    random.randint( 80_000, 4_000_000),
        }
    return quotes
