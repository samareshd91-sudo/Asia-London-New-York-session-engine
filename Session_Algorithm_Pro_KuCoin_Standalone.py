# ================================================================
# SESSION ALGORITHM PRO — KUCOIN FUTURES / DB-FREE / SINGLE FILE
# ================================================================
# Single-file Streamlit application.
#
# Install once:
#   pip install streamlit pandas requests websocket-client
#
# Run:
#   streamlit run Session_Algorithm_Pro_KuCoin_Standalone.py
#
# Environment variables / Streamlit secrets:
#   TELEGRAM_BOT_TOKEN
#   TELEGRAM_CHAT_ID
#
# No PostgreSQL / Neon / SQLite / Binance dependency.
# Market data: KuCoin Futures WebSocket + KuCoin Futures REST bootstrap.
#
# IMPORTANT:
# This is an alert/research engine. It does NOT place exchange orders.
# ================================================================

import os
import json
import time
import uuid
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque

import requests
import pandas as pd
import streamlit as st
import websocket


# ================================================================
# STREAMLIT CONFIG
# ================================================================
st.set_page_config(
    page_title="Session Algorithm Pro — KuCoin",
    page_icon="🟣",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ================================================================
# CONFIG
# ================================================================
KUCOIN_FUTURES_TOKEN_URL = (
    "https://api-futures.kucoin.com/api/v1/bullet-public"
)
KUCOIN_FUTURES_KLINE_URL = (
    "https://api-futures.kucoin.com/api/v1/kline/query"
)

DEFAULT_SYMBOLS = [
    "XBTUSDTM",
    "ETHUSDTM",
    "SOLUSDTM",
    "BNBUSDTM",
    "XRPUSDTM",
    "DOGEUSDTM",
]

CANDLE_INTERVAL = "1min"
MAX_CANDLES_PER_SYMBOL = 900
SCAN_EVERY_SECONDS = 3
TELEGRAM_TIMEOUT = 15

# UTC session model.
ASIA_START = 0
ASIA_END = 8

LONDON_START = 8
LONDON_END = 13

NEW_YORK_START = 13
NEW_YORK_END = 21


# ================================================================
# DATA MODEL
# ================================================================
@dataclass
class Signal:
    symbol: str
    direction: str
    session: str
    entry: float
    stop_loss: float
    take_profit: float
    score: int
    setup: str
    candle_time: str
    reference: str
    reference_high: float
    reference_low: float

    def dashboard_text(self):
        emoji = "🟢" if self.direction == "LONG" else "🔴"
        return (
            f"{emoji} {self.session} — {self.direction}\n"
            f"{self.symbol} | Score {self.score}/100\n"
            f"Entry {self.entry:.8f} | SL {self.stop_loss:.8f} | "
            f"TP {self.take_profit:.8f}\n"
            f"{self.setup}"
        )

    def telegram_text(self):
        emoji = "🟢" if self.direction == "LONG" else "🔴"
        return (
            f"{emoji} <b>SESSION ALGORITHM PRO</b>\n\n"
            f"<b>{self.session} — {self.direction} SIGNAL</b>\n\n"
            f"<b>Coin:</b> {self.symbol}\n"
            f"<b>Entry:</b> {self.entry:.8f}\n"
            f"<b>Stop Loss:</b> {self.stop_loss:.8f}\n"
            f"<b>Take Profit:</b> {self.take_profit:.8f}\n"
            f"<b>Score:</b> {self.score}/100\n"
            f"<b>Reference:</b> {self.reference}\n\n"
            f"<b>Setup:</b> {self.setup}\n\n"
            f"<b>UTC:</b> {self.candle_time}\n\n"
            f"<i>Analytical alert — not financial advice.</i>"
        )


# ================================================================
# TELEGRAM
# ================================================================
class Telegram:
    def __init__(self, token="", chat_id=""):
        self.token = str(token or "").strip()
        self.chat_id = str(chat_id or "").strip()

    @property
    def configured(self):
        return bool(self.token and self.chat_id)

    def send(self, text):
        if not self.configured:
            return False, "Telegram is not configured."

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            r = requests.post(url, json=payload, timeout=TELEGRAM_TIMEOUT)
            data = r.json()

            if r.ok and data.get("ok"):
                return True, "sent"

            return False, f"Telegram API: {data}"
        except Exception as exc:
            return False, f"Telegram exception: {exc}"


# ================================================================
# KUCOIN FUTURES STREAM
# ================================================================
class KuCoinFuturesStream:
    """
    DB-free, in-memory KuCoin Futures market-data engine.

    Bootstrap:
      REST historical 1-minute candles.

    Live:
      KuCoin Futures public WebSocket K-line channel.

    The WebSocket updates the current candle. The application keeps
    the latest candles in RAM only.
    """

    def __init__(self, symbols):
        self.symbols = list(dict.fromkeys(symbols))
        self.lock = threading.RLock()

        self.candles = {
            symbol: deque(maxlen=MAX_CANDLES_PER_SYMBOL)
            for symbol in self.symbols
        }

        self.last_price = {}
        self.last_update = {}
        self.status = "STARTING"
        self.last_error = ""
        self.ws = None
        self.thread = None
        self.stop_event = threading.Event()
        self.started = False

    # ----------------------------
    # REST bootstrap
    # ----------------------------
    def bootstrap(self):
        for symbol in self.symbols:
            try:
                params = {
                    "symbol": symbol,
                    "type": CANDLE_INTERVAL,
                }

                r = requests.get(
                    KUCOIN_FUTURES_KLINE_URL,
                    params=params,
                    timeout=15,
                )
                r.raise_for_status()
                payload = r.json()

                rows = payload.get("data", [])
                parsed = []

                for row in rows:
                    if len(row) < 7:
                        continue

                    # KuCoin Futures classic Kline:
                    # [time, open, close, high, low, volume, amount]
                    parsed.append({
                        "ts": int(float(row[0])),
                        "open": float(row[1]),
                        "close": float(row[2]),
                        "high": float(row[3]),
                        "low": float(row[4]),
                        "volume": float(row[5]),
                        "amount": float(row[6]),
                    })

                parsed.sort(key=lambda x: x["ts"])

                with self.lock:
                    self.candles[symbol].clear()
                    self.candles[symbol].extend(parsed)

                    if parsed:
                        self.last_price[symbol] = parsed[-1]["close"]
                        self.last_update[symbol] = time.time()

            except Exception as exc:
                self.last_error = f"{symbol} bootstrap: {exc}"

    # ----------------------------
    # WebSocket token
    # ----------------------------
    def get_public_token(self):
        r = requests.post(
            KUCOIN_FUTURES_TOKEN_URL,
            timeout=15,
        )
        r.raise_for_status()

        payload = r.json()

        if payload.get("code") != "200000":
            raise RuntimeError(f"KuCoin token response: {payload}")

        data = payload["data"]
        token = data["token"]
        server = data["instanceServers"][0]

        return token, server

    # ----------------------------
    # WebSocket callbacks
    # ----------------------------
    def on_open(self, ws):
        self.status = "CONNECTED"

        topic_symbols = ",".join(
            f"{symbol}_{CANDLE_INTERVAL}"
            for symbol in self.symbols
        )

        message = {
            "id": str(uuid.uuid4().hex[:20]),
            "type": "subscribe",
            "topic": f"/contractMarket/limitCandle:{topic_symbols}",
            "response": True,
        }

        ws.send(json.dumps(message))

    def on_message(self, ws, raw):
        try:
            msg = json.loads(raw)

            if msg.get("type") == "pong":
                return

            data = msg.get("data") or {}
            candles = data.get("candles")

            if not candles:
                return

            symbol = data.get("symbol")
            if symbol not in self.candles:
                return

            ts = int(float(candles[0]))
            row = {
                "ts": ts,
                "open": float(candles[1]),
                "close": float(candles[2]),
                "high": float(candles[3]),
                "low": float(candles[4]),
                # KuCoin notes the Futures candle volume field may be
                # incorrect in this classic channel, so keep it but
                # do not use it as a hard signal requirement.
                "volume": float(candles[5]),
                "amount": float(candles[6]),
            }

            with self.lock:
                q = self.candles[symbol]

                if q and q[-1]["ts"] == ts:
                    q[-1] = row
                elif not q or ts > q[-1]["ts"]:
                    q.append(row)

                self.last_price[symbol] = row["close"]
                self.last_update[symbol] = time.time()

        except Exception as exc:
            self.last_error = f"WS message: {exc}"

    def on_error(self, ws, error):
        self.status = "ERROR"
        self.last_error = f"WebSocket: {error}"

    def on_close(self, ws, code, msg):
        self.status = "DISCONNECTED"

    # ----------------------------
    # Ping
    # ----------------------------
    def ping_loop(self, ws, interval):
        while not self.stop_event.wait(max(5, int(interval * 0.6))):
            try:
                ws.send(json.dumps({
                    "id": str(uuid.uuid4().hex[:20]),
                    "type": "ping",
                }))
            except Exception:
                break

    # ----------------------------
    # Worker
    # ----------------------------
    def _worker(self):
        self.bootstrap()

        while not self.stop_event.is_set():
            try:
                token, server = self.get_public_token()

                endpoint = server["endpoint"].rstrip("/")
                ping_interval = int(server.get("pingInterval", 18000)) / 1000

                url = f"{endpoint}/?token={token}"

                self.status = "CONNECTING"

                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )

                ping_thread = threading.Thread(
                    target=self.ping_loop,
                    args=(self.ws, ping_interval),
                    daemon=True,
                )
                ping_thread.start()

                self.ws.run_forever(
                    ping_interval=None,
                    ping_timeout=None,
                )

            except Exception as exc:
                self.status = "RECONNECTING"
                self.last_error = f"WS worker: {exc}"

            if not self.stop_event.wait(3):
                continue

    def start(self):
        if self.started:
            return

        self.started = True
        self.stop_event.clear()

        self.thread = threading.Thread(
            target=self._worker,
            daemon=True,
            name="kucoin-futures-stream",
        )
        self.thread.start()

    def stop(self):
        self.stop_event.set()

        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

        self.status = "STOPPED"

    # ----------------------------
    # Snapshot
    # ----------------------------
    def dataframe(self, symbol):
        with self.lock:
            rows = list(self.candles.get(symbol, []))

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df["datetime"] = pd.to_datetime(
            df["ts"],
            unit="s",
            utc=True,
        )
        return df


# ================================================================
# SESSION ALGORITHM
# ================================================================
class SessionAlgorithm:
    def __init__(self):
        self.signals = deque(maxlen=100)
        self.seen_signal_keys = set()
        self.telegram_last_sent = {}
        self.session_snapshots = {}
        self.errors = deque(maxlen=30)

    @staticmethod
    def current_session(dt):
        h = dt.hour

        if ASIA_START <= h < ASIA_END:
            return "ASIA"

        if LONDON_START <= h < LONDON_END:
            return "LONDON"

        if NEW_YORK_START <= h < NEW_YORK_END:
            return "NEW YORK"

        return "OFF"

    @staticmethod
    def atr(df, period=14):
        if len(df) < period + 2:
            return None

        pc = df["close"].shift(1)

        tr = pd.concat(
            [
                df["high"] - df["low"],
                (df["high"] - pc).abs(),
                (df["low"] - pc).abs(),
            ],
            axis=1,
        ).max(axis=1)

        value = tr.rolling(period).mean().iloc[-1]

        if pd.isna(value) or value <= 0:
            return None

        return float(value)

    @staticmethod
    def add_indicators(df):
        out = df.copy()

        out["ema20"] = out["close"].ewm(
            span=20,
            adjust=False,
        ).mean()

        out["ema50"] = out["close"].ewm(
            span=50,
            adjust=False,
        ).mean()

        out["body"] = (out["close"] - out["open"]).abs()

        return out

    @staticmethod
    def session_range(df, start_hour, end_hour):
        mask = (
            (df["datetime"].dt.hour >= start_hour)
            & (df["datetime"].dt.hour < end_hour)
        )

        part = df.loc[mask]

        if part.empty:
            return None, None

        return (
            float(part["high"].max()),
            float(part["low"].min()),
        )

    def build_snapshot(self, df):
        asia_h, asia_l = self.session_range(
            df, ASIA_START, ASIA_END
        )

        london_h, london_l = self.session_range(
            df, LONDON_START, LONDON_END
        )

        ny_h, ny_l = self.session_range(
            df, NEW_YORK_START, NEW_YORK_END
        )

        current = self.current_session(
            df.iloc[-1]["datetime"].to_pydatetime()
        )

        return {
            "session": current,
            "ASIA": {
                "high": asia_h,
                "low": asia_l,
            },
            "LONDON": {
                "high": london_h,
                "low": london_l,
            },
            "NEW YORK": {
                "high": ny_h,
                "low": ny_l,
            },
        }

    def detect(
        self,
        symbol,
        df,
        min_score=70,
        risk_reward=2.0,
    ):
        if len(df) < 80:
            return None

        df = self.add_indicators(df)

        # Ignore the currently forming candle for the actual signal.
        # The last closed candle is used.
        last = df.iloc[-2]
        prev = df.iloc[-3]

        session = self.current_session(
            last["datetime"].to_pydatetime()
        )

        if session == "OFF":
            return None

        atr = self.atr(df.iloc[:-1])

        if not atr or atr <= 0:
            return None

        snapshot = self.build_snapshot(df.iloc[:-1])

        # London trades Asia liquidity.
        if session == "LONDON":
            ref_name = "ASIA"
            ref = snapshot["ASIA"]

        # New York trades London liquidity.
        elif session == "NEW YORK":
            ref_name = "LONDON"
            ref = snapshot["LONDON"]

        # Asia is mainly range-building in this model.
        # We can still alert on a rolling-range sweep.
        else:
            ref_name = "ROLLING"
            recent = df.iloc[-51:-2]
            if recent.empty:
                return None

            ref = {
                "high": float(recent["high"].max()),
                "low": float(recent["low"].min()),
            }

        if ref["high"] is None or ref["low"] is None:
            return None

        ref_high = float(ref["high"])
        ref_low = float(ref["low"])

        # --------------------------------------------------------
        # Liquidity sweep
        # --------------------------------------------------------
        low_sweep = (
            float(prev["low"]) < ref_low
            and float(last["close"]) > ref_low
        )

        high_sweep = (
            float(prev["high"]) > ref_high
            and float(last["close"]) < ref_high
        )

        if not low_sweep and not high_sweep:
            return None

        long_score = 30 if low_sweep else 0
        short_score = 30 if high_sweep else 0

        long_reasons = []
        short_reasons = []

        if low_sweep:
            long_reasons.append(
                f"{ref_name} low liquidity sweep + reclaim"
            )

        if high_sweep:
            short_reasons.append(
                f"{ref_name} high liquidity sweep + rejection"
            )

        # --------------------------------------------------------
        # Displacement
        # --------------------------------------------------------
        body = float(last["body"])

        if body >= 0.60 * atr:
            if last["close"] > last["open"]:
                long_score += 15
                long_reasons.append("bullish displacement")

            elif last["close"] < last["open"]:
                short_score += 15
                short_reasons.append("bearish displacement")

        # --------------------------------------------------------
        # Local structure break
        # --------------------------------------------------------
        local = df.iloc[-8:-2]

        if not local.empty:
            local_high = float(local["high"].max())
            local_low = float(local["low"].min())

            if float(last["close"]) > local_high:
                long_score += 20
                long_reasons.append(
                    "local bullish structure break"
                )

            if float(last["close"]) < local_low:
                short_score += 20
                short_reasons.append(
                    "local bearish structure break"
                )

        # --------------------------------------------------------
        # EMA regime
        # --------------------------------------------------------
        if (
            float(last["close"])
            > float(last["ema20"])
            > float(last["ema50"])
        ):
            long_score += 15
            long_reasons.append("EMA bullish alignment")

        if (
            float(last["close"])
            < float(last["ema20"])
            < float(last["ema50"])
        ):
            short_score += 15
            short_reasons.append("EMA bearish alignment")

        # --------------------------------------------------------
        # Volume is deliberately soft here.
        # KuCoin's classic Futures candle documentation warns that
        # the candle volume field may be incorrect. Therefore we
        # DO NOT award score from that field.
        # --------------------------------------------------------

        if long_score < min_score and short_score < min_score:
            return None

        if long_score >= short_score:
            direction = "LONG"
            score = min(long_score, 100)
            reasons = long_reasons
            entry = float(last["close"])

            stop_loss = min(
                float(prev["low"]),
                ref_low,
                entry - 0.80 * atr,
            )

            risk = entry - stop_loss

            if risk <= 0:
                return None

            take_profit = entry + risk * risk_reward

        else:
            direction = "SHORT"
            score = min(short_score, 100)
            reasons = short_reasons
            entry = float(last["close"])

            stop_loss = max(
                float(prev["high"]),
                ref_high,
                entry + 0.80 * atr,
            )

            risk = stop_loss - entry

            if risk <= 0:
                return None

            take_profit = entry - risk * risk_reward

        candle_time = last["datetime"].strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )

        signal = Signal(
            symbol=symbol,
            direction=direction,
            session=session,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            score=int(score),
            setup=" → ".join(reasons),
            candle_time=candle_time,
            reference=ref_name,
            reference_high=ref_high,
            reference_low=ref_low,
        )

        # Same candle/direction/symbol = one signal only.
        key = (
            f"{symbol}|{direction}|"
            f"{session}|{int(last['ts'])}"
        )

        if key in self.seen_signal_keys:
            return None

        self.seen_signal_keys.add(key)
        self.signals.appendleft(signal)

        return signal


# ================================================================
# RESOURCE INITIALIZATION
# ================================================================
def get_secret(name, default=""):
    try:
        value = st.secrets.get(name, default)
        if value:
            return value
    except Exception:
        pass

    return os.getenv(name, default)


@st.cache_resource
def get_runtime(symbols_tuple):
    stream = KuCoinFuturesStream(list(symbols_tuple))
    engine = SessionAlgorithm()
    telegram = Telegram(
        get_secret("TELEGRAM_BOT_TOKEN"),
        get_secret("TELEGRAM_CHAT_ID"),
    )

    stream.start()

    return stream, engine, telegram


# ================================================================
# SIDEBAR
# ================================================================
st.sidebar.title("🟣 Session Algorithm Pro")

symbols_text = st.sidebar.text_area(
    "KuCoin Futures Symbols",
    value="\n".join(DEFAULT_SYMBOLS),
    height=170,
    help=(
        "Use KuCoin Futures contract symbols, e.g. "
        "XBTUSDTM, ETHUSDTM."
    ),
)

symbols = [
    x.strip().upper()
    for x in symbols_text.splitlines()
    if x.strip()
]

min_score = st.sidebar.slider(
    "Minimum Signal Score",
    50,
    90,
    70,
    1,
)

risk_reward = st.sidebar.slider(
    "Risk / Reward",
    1.0,
    5.0,
    2.0,
    0.1,
)

telegram_enabled = st.sidebar.checkbox(
    "Enable Telegram",
    value=bool(
        get_secret("TELEGRAM_BOT_TOKEN")
        and get_secret("TELEGRAM_CHAT_ID")
    ),
)

telegram_cooldown = st.sidebar.slider(
    "Telegram cooldown / symbol",
    5,
    180,
    30,
    5,
)

st.sidebar.divider()

st.sidebar.markdown("### UTC Sessions")
st.sidebar.write("🇯🇵 Asia — 00:00 → 08:00")
st.sidebar.write("🇬🇧 London — 08:00 → 13:00")
st.sidebar.write("🇺🇸 New York — 13:00 → 21:00")

st.sidebar.divider()

st.sidebar.caption("Market: KuCoin Futures")
st.sidebar.caption("Stream: WebSocket")
st.sidebar.caption("Storage: RAM only")
st.sidebar.caption("Database: None")


# ================================================================
# RUNTIME
# ================================================================
stream, engine, telegram = get_runtime(tuple(symbols))

now = datetime.now(timezone.utc)

st.title("🟣 Session Algorithm Pro")
st.caption(
    "Standalone • KuCoin Futures WebSocket • No Database • Telegram Alerts"
)

# ================================================================
# TOP METRICS
# ================================================================
current_session = engine.current_session(now)

m1, m2, m3, m4, m5 = st.columns(5)

m1.metric("UTC", now.strftime("%H:%M:%S"))
m2.metric("Session", current_session)
m3.metric("Stream", stream.status)
m4.metric("Symbols", len(symbols))
m5.metric("Signals", len(engine.signals))


# ================================================================
# CONNECTION STATUS
# ================================================================
if stream.status == "CONNECTED":
    st.success("🟢 KuCoin Futures WebSocket connected")
elif stream.status in ("CONNECTING", "STARTING", "RECONNECTING"):
    st.warning(f"🟡 KuCoin stream: {stream.status}")
else:
    st.error(f"🔴 KuCoin stream: {stream.status}")

if stream.last_error:
    with st.expander("Stream diagnostics"):
        st.code(stream.last_error)


# ================================================================
# LIVE SESSION BOARD
# ================================================================
st.subheader("📊 Live Session Board")

rows = []
new_signals = []

for symbol in symbols:
    try:
        df = stream.dataframe(symbol)

        if df.empty:
            rows.append({
                "Coin": symbol,
                "Price": "WAIT",
                "Session": current_session,
                "Asia": "—",
                "London": "—",
                "NY": "—",
                "Signal": "WAIT",
                "Score": 0,
            })
            continue

        snapshot = engine.build_snapshot(df)

        signal = engine.detect(
            symbol,
            df,
            min_score=min_score,
            risk_reward=risk_reward,
        )

        price = float(df.iloc[-1]["close"])

        def fmt_range(name):
            item = snapshot[name]
            if item["high"] is None:
                return "—"
            return (
                f"{item['low']:.6g} ↔ "
                f"{item['high']:.6g}"
            )

        rows.append({
            "Coin": symbol,
            "Price": price,
            "Session": snapshot["session"],
            "Asia": fmt_range("ASIA"),
            "London": fmt_range("LONDON"),
            "NY": fmt_range("NEW YORK"),
            "Signal": signal.direction if signal else "WAIT",
            "Score": signal.score if signal else 0,
        })

        if signal:
            new_signals.append(signal)

            # Telegram cooldown.
            key = f"{signal.symbol}|{signal.direction}"
            last_sent = engine.telegram_last_sent.get(key, 0)

            if (
                telegram_enabled
                and telegram.configured
                and time.time() - last_sent
                >= telegram_cooldown * 60
            ):
                ok, msg = telegram.send(
                    signal.telegram_text()
                )

                if ok:
                    engine.telegram_last_sent[key] = time.time()
                else:
                    engine.errors.append(
                        f"Telegram {signal.symbol}: {msg}"
                    )

    except Exception as exc:
        engine.errors.append(
            f"{symbol}: {type(exc).__name__}: {exc}"
        )


if rows:
    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True,
        hide_index=True,
    )


# ================================================================
# SESSION PANELS
# ================================================================
st.subheader("🗺️ Session Map")

for symbol in symbols:
    df = stream.dataframe(symbol)

    if df.empty:
        continue

    snap = engine.build_snapshot(df)

    with st.expander(
        f"{symbol} — {snap['session']}",
        expanded=False,
    ):
        c1, c2, c3 = st.columns(3)

        for col, name in zip(
            (c1, c2, c3),
            ("ASIA", "LONDON", "NEW YORK"),
        ):
            item = snap[name]

            with col:
                st.markdown(f"### {name}")

                if item["high"] is None:
                    st.write("No range yet")
                else:
                    st.metric(
                        "High",
                        f"{item['high']:.8f}",
                    )
                    st.metric(
                        "Low",
                        f"{item['low']:.8f}",
                    )


# ================================================================
# ALERTS
# ================================================================
st.subheader("🚨 Signal Alerts")

if new_signals:
    for signal in new_signals:
        if signal.direction == "LONG":
            st.success(signal.dashboard_text())
        else:
            st.error(signal.dashboard_text())
else:
    st.info(
        "No new qualified session signal on this refresh."
    )


# ================================================================
# SIGNAL HISTORY
# ================================================================
st.subheader("📜 Recent Signals")

if engine.signals:
    history = []

    for s in list(engine.signals)[:50]:
        history.append({
            "Time": s.candle_time,
            "Coin": s.symbol,
            "Direction": s.direction,
            "Session": s.session,
            "Entry": s.entry,
            "SL": s.stop_loss,
            "TP": s.take_profit,
            "Score": s.score,
            "Reference": s.reference,
            "Setup": s.setup,
        })

    st.dataframe(
        pd.DataFrame(history),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No signals yet.")


# ================================================================
# TELEGRAM STATUS
# ================================================================
with st.expander("📲 Telegram"):
    if telegram.configured:
        if telegram_enabled:
            st.success("Telegram configured and enabled.")
        else:
            st.warning(
                "Telegram credentials found, but sending is disabled."
            )
    else:
        st.warning(
            "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID "
            "in environment variables or Streamlit secrets."
        )


# ================================================================
# ERRORS
# ================================================================
with st.expander("🛠 Runtime Diagnostics"):
    st.write("Database: NONE")
    st.write("Exchange: KuCoin Futures")
    st.write("WebSocket: " + stream.status)
    st.write("Storage: In-memory only")

    if engine.errors:
        for error in list(engine.errors)[-20:]:
            st.code(error)

    if stream.last_error:
        st.code(stream.last_error)


# ================================================================
# AUTO REFRESH
# ================================================================
time.sleep(SCAN_EVERY_SECONDS)
st.rerun()
