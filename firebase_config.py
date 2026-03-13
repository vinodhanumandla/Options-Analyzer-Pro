"""
Firebase Configuration for Options Analyzer Pro
Handles Firebase Admin SDK initialization and credential storage.
"""
import firebase_admin
from firebase_admin import credentials, db
import os
import json

_firebase_app = None

def init_firebase(db_url=None):
    """Initialize Firebase Admin SDK."""
    global _firebase_app
    if _firebase_app is not None:
        return True
    
    cred_path = os.path.join(os.path.dirname(__file__), 'firebase-credentials.json')
    
    if not os.path.exists(cred_path):
        print("[Firebase] WARNING: firebase-credentials.json not found. Running without Firebase.")
        return False
    
    if db_url is None:
        db_url = os.environ.get('FIREBASE_DB_URL', '')
    
    if not db_url:
        print("[Firebase] WARNING: No database URL. Running without Firebase.")
        return False
    
    try:
        cred = credentials.Certificate(cred_path)
        _firebase_app = firebase_admin.initialize_app(cred, {
            'databaseURL': db_url
        })
        print("[Firebase] Initialized successfully.")
        return True
    except Exception as e:
        print(f"[Firebase] Init error: {e}")
        return False


def save_credentials(app_id, access_token):
    """Save Fyers credentials to Firebase."""
    if _firebase_app is None:
        return False
    try:
        ref = db.reference('/fyers_credentials')
        ref.set({
            'app_id': app_id,
            'access_token': access_token,
            'connected': True
        })
        return True
    except Exception as e:
        print(f"[Firebase] Save error: {e}")
        return False


def load_credentials():
    """Load saved Fyers credentials from Firebase."""
    if _firebase_app is None:
        return None
    try:
        ref = db.reference('/fyers_credentials')
        data = ref.get()
        return data
    except Exception as e:
        print(f"[Firebase] Load error: {e}")
        return None


def clear_credentials():
    """Clear credentials from Firebase on disconnect."""
    if _firebase_app is None:
        return False
    try:
        ref = db.reference('/fyers_credentials')
        ref.set({
            'app_id': '',
            'access_token': '',
            'connected': False
        })
        return True
    except Exception as e:
        print(f"[Firebase] Clear error: {e}")
        return False
