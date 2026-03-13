# Options Analyzer Pro

An advanced algorithmic trading dashboard and scanner for NSE F&O stocks. This system identifies high-probability trading setups using Demand/Supply Zones, Market Structure (HH/HL), Gap analysis, and Candlestick Confluence.

## Features

- **Live Market Dashboard**: Real-time tracking of Nifty 500 F&O stocks.
- **D/S Zone Strategy**: Institutional-grade scanner for Demand/Supply zones and Gap Breakouts.
- **R-Factor Analysis**: Identifies high-momentum (Buy) and weak-momentum (Sell) stocks across all sectors.
- **Q3 Results Scanner**: Automated tracking of corporate earnings, beats/misses, and market sentiment.
- **TradingView Integration**: One-click chart analysis directly from the dashboard.
- **Multi-Timeframe Scanning**: Analyzes 1m, 5m, and 15m charts simultaneously in the background.

## Technology Stack

- **Backend**: Python (Flask)
- **Frontend**: HTML5, Vanilla CSS, JavaScript (Modern ES6)
- **Data Source**: YFinance / Fyers API
- **Database**: Firebase Realtime Database (for credential persistence)

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/options-analyzer-pro.git
   cd options-analyzer-pro
   ```

2. **Setup Virtual Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Configuration**:
   - Copy `.env.example` to `.env`.
   - Update your `FIREBASE_DB_URL` and `SECRET_KEY`.
   - Place your `firebase-credentials.json` in the root directory.

## Usage

Start the Flask application:
```bash
python app.py
```
Open your browser and navigate to `http://127.0.0.1:5000`.

## GitHub Deployment Checklist

- [x] Sensitive API keys moved to `.env` (handled via Fyers login flow).
- [x] `.gitignore` configured to exclude venv, logs, and secrets.
- [x] `requirements.txt` updated with all dependencies.
- [x] `README.md` documentation complete.

## Disclaimer

*Trading in financial markets involves high risk. This tool is for educational and informational purposes only. Always consult with a certified financial advisor before making investment decisions.*
