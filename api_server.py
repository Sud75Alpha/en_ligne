"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         API SERVER v3.1 — GOLD/DXY BOT                                      ║
║                                                                              ║
║  Lancement : python api_server.py                                            ║
║  HTTP REST : http://0.0.0.0:8000                                             ║
║  WebSocket : ws://0.0.0.0:8000/ws?api_key=...                               ║
║                                                                              ║
║  FIX v3.1 :                                                                  ║
║  ✅ Ajout endpoint POST /api/snapshot/push (bot → API)                       ║
║  ✅ Snapshot pushé par le bot appliqué directement à STATE                   ║
║  ✅ Dashboard reçoit toutes les données depuis le bot                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os, json, time, asyncio, logging, threading
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Set, Any
from collections import deque

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

MT5_AVAILABLE = False
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    pass

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

API_HOST = "0.0.0.0"
API_PORT = int(os.getenv("API_PORT", 8000))
API_KEY  = os.getenv("API_KEY", "gold_dxy_secret_2024")

GOLD_SYMBOLS    = ["XAUUSD", "GOLD", "XAUUSDm", "XAU/USD"]
DXY_SYMBOLS     = ["DXY", "USDX", "USDIndex", "DX-Y.NYB", "DXYF", "DXYm"]
CORR_WINDOW     = 50
MAX_LOG_ENTRIES = 200
MAX_SIGNALS     = 100

WS_PRICE_INTERVAL = 0.5
WS_OHLCV_INTERVAL = 8.0
WS_ZONES_INTERVAL = 10.0

logging.basicConfig(level=logging.INFO,
    format="[%(asctime)s] %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("APIServer")

# ─────────────────────────────────────────────────────────────────────────────
#  WEBSOCKET MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.active.add(ws)
        log.info(f"WS connecté — total: {len(self.active)}")

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self.active.discard(ws)

    async def broadcast(self, data: dict):
        if not self.active: return
        msg  = json.dumps(data, default=str)
        dead = set()
        async with self._lock:
            clients = set(self.active)
        for ws in clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.add(ws)
        if dead:
            async with self._lock:
                self.active -= dead

    async def send_to(self, ws: WebSocket, data: dict):
        try:
            await ws.send_text(json.dumps(data, default=str))
        except Exception:
            await self.disconnect(ws)

    @property
    def count(self): return len(self.active)


ws_manager = ConnectionManager()

# ─────────────────────────────────────────────────────────────────────────────
#  STATE GLOBAL
# ─────────────────────────────────────────────────────────────────────────────

class GlobalState:
    def __init__(self):
        self._lock = threading.RLock()
        # Prix
        self.gold_price: float = 0.0
        self.dxy_price:  float = 0.0
        self.gold_bid:   float = 0.0
        self.gold_ask:   float = 0.0
        self.gold_prev:  float = 0.0
        self.dxy_prev:   float = 0.0
        self.gold_change: float = 0.0
        self.gold_pct:    float = 0.0
        self.dxy_change:  float = 0.0
        self.correlation: float = -0.75
        self.corr_history: List[Dict] = []
        # Signal
        self.current_signal: Dict = {
            "direction": "WAIT", "anticipation": None, "confidence": 0,
            "corr": -0.75, "gold_price": 0.0, "dxy_price": 0.0,
            "entry": 0.0, "tp": 0.0, "sl": 0.0, "rr": 0.0,
            "sl_source": "—", "pipeline_state": "IDLE",
            "time": datetime.now().isoformat(),
        }
        self._last_signal_hash: str = ""
        # Historique
        self.signals: List[Dict] = []
        self._load_signals()
        # Logs
        self.bot_logs: deque = deque(maxlen=MAX_LOG_ENTRIES)
        self.bot_logs.append({"time": datetime.now().strftime("%H:%M:%S"),
                               "level": "INFO", "msg": "API Server v3.1 démarré"})
        # OHLCV + zones + MTF
        self.ohlcv: Dict[str, List[Dict]] = {"M5": [], "M15": [], "H1": []}
        self.zones: Dict = {
            "support": 0.0, "resistance": 0.0,
            "fvg_bullish": [], "fvg_bearish": [],
            "ob_buy": None, "ob_sell": None,
            "swing_lows": [], "swing_highs": [], "atr": 0.0, "fvg_filter": 0.0,
        }
        self.mtf_analysis: Dict = {
            "H1":  {"signal": "WAIT", "corr": 0.0, "trend": "—", "anticipation": None},
            "M15": {"signal": "WAIT", "corr": 0.0, "trend": "—", "anticipation": None},
            "M5":  {"signal": "WAIT", "corr": 0.0, "trend": "—", "anticipation": None},
        }
        # Stats
        self.winrate: float = 0.0
        self.wins:    int   = 0
        self.losses:  int   = 0
        # Statut
        self.bot_status:    str           = "starting"
        self.mt5_connected: bool          = False
        self.gold_symbol:   Optional[str] = None
        self.dxy_symbol:    Optional[str] = None
        self.last_update:   str           = datetime.now().isoformat()
        # Flag données venant du bot
        self.bot_data_received: bool = False

    def add_log(self, level: str, msg: str):
        with self._lock:
            self.bot_logs.append({
                "time": datetime.now().strftime("%H:%M:%S"),
                "level": level, "msg": msg,
            })

    def set_signal(self, data: Dict):
        with self._lock:
            self.current_signal = {**self.current_signal, **data,
                                   "time": datetime.now().isoformat()}

    def signal_changed(self) -> bool:
        h = f"{self.current_signal.get('direction')}{self.current_signal.get('confidence')}"
        if h != self._last_signal_hash:
            self._last_signal_hash = h; return True
        return False

    def apply_bot_snapshot(self, data: Dict):
        """Applique un snapshot complet envoyé par le bot."""
        with self._lock:
            # Prix
            for k in ["gold_price","dxy_price","gold_bid","gold_ask",
                       "gold_change","gold_pct","dxy_change","correlation",
                       "bot_status","mt5_connected","gold_symbol","last_update"]:
                if k in data:
                    setattr(self, k, data[k])

            # Conserver gold_prev/dxy_prev pour les frames WS
            if "gold_price" in data and data["gold_price"]:
                self.gold_prev = self.gold_prev or data["gold_price"]
            if "dxy_price" in data and data["dxy_price"]:
                self.dxy_prev = self.dxy_prev or data["dxy_price"]

            # Signal
            if "signal" in data and isinstance(data["signal"], dict):
                self.current_signal = {**self.current_signal, **data["signal"],
                                       "time": datetime.now().isoformat()}

            # MTF
            if "mtf" in data and isinstance(data["mtf"], dict):
                for tf_n, tf_data in data["mtf"].items():
                    if tf_n in self.mtf_analysis and isinstance(tf_data, dict):
                        self.mtf_analysis[tf_n] = {**self.mtf_analysis[tf_n], **tf_data}

            # OHLCV
            if "ohlcv" in data and isinstance(data["ohlcv"], dict):
                for tf_n, candles in data["ohlcv"].items():
                    if candles and isinstance(candles, list):
                        self.ohlcv[tf_n] = candles

            # Signaux historique
            if "signals" in data and isinstance(data["signals"], list):
                # Fusion (garde les plus récents, déduplique par time)
                existing_times = {s.get("time") for s in self.signals}
                for s in data["signals"]:
                    if s.get("time") not in existing_times:
                        self.signals.append(s)
                self.signals = self.signals[-MAX_SIGNALS:]

            # Stats
            if "winrate" in data: self.winrate = data["winrate"]
            if "wins"    in data: self.wins    = data["wins"]
            if "losses"  in data: self.losses  = data["losses"]

            self.last_update        = datetime.now().isoformat()
            self.bot_data_received  = True

            # Zones depuis OHLCV M5 si pas encore calculées
            if self.ohlcv.get("M5") and not self.zones.get("atr"):
                self.zones = compute_zones(self.ohlcv["M5"])

    def add_signal_to_history(self, signal: Dict):
        with self._lock:
            signal["id"] = int(time.time() * 1000)
            self.signals.append(signal)
            if len(self.signals) > MAX_SIGNALS:
                self.signals = self.signals[-MAX_SIGNALS:]
            self._save_signals()
            self._update_winrate()

    def _update_winrate(self):
        closed = [s for s in self.signals if s.get("result") in ("WIN","LOSS")]
        if closed:
            self.wins    = sum(1 for s in closed if s["result"] == "WIN")
            self.losses  = len(closed) - self.wins
            self.winrate = round(self.wins / len(closed) * 100, 1)

    def _load_signals(self):
        try:
            if os.path.exists("signals.json"):
                with open("signals.json") as f:
                    self.signals = json.load(f)
                self._update_winrate()
                log.info(f"Signaux chargés: {len(self.signals)}")
        except Exception:
            self.signals = []

    def _save_signals(self):
        try:
            with open("signals.json", "w") as f:
                json.dump(self.signals, f, indent=2, default=str)
        except Exception:
            pass

    # ── Frames WebSocket ───────────────────────────────────────────────────────

    def price_frame(self) -> Dict:
        with self._lock:
            return {
                "type": "price",
                "gold": self.gold_price,     "dxy": self.dxy_price,
                "gold_bid": self.gold_bid,   "gold_ask": self.gold_ask,
                "gold_change": self.gold_change,
                "gold_pct":   self.gold_pct,
                "dxy_change": self.dxy_change,
                "corr": self.correlation,
                "ts":   datetime.now().isoformat(),
            }

    def signal_frame(self) -> Dict:
        with self._lock:
            return {
                "type": "signal",
                "signal": dict(self.current_signal),
                "mtf":    dict(self.mtf_analysis),
                "stats":  {"winrate": self.winrate, "wins": self.wins,
                            "losses": self.losses, "total": len(self.signals)},
                "ts": datetime.now().isoformat(),
            }

    def ohlcv_frame(self, tf: str = "M5") -> Dict:
        with self._lock:
            return {"type": "ohlcv", "timeframe": tf,
                    "candles": self.ohlcv.get(tf, []),
                    "ts": datetime.now().isoformat()}

    def zones_frame(self) -> Dict:
        with self._lock:
            return {"type": "zones", "zones": dict(self.zones),
                    "ts": datetime.now().isoformat()}

    def logs_frame(self) -> Dict:
        with self._lock:
            return {"type": "logs", "logs": list(self.bot_logs)[-30:],
                    "ts": datetime.now().isoformat()}

    def full_snapshot(self) -> Dict:
        with self._lock:
            return {
                "type":          "snapshot",
                "gold_price":    self.gold_price,
                "dxy_price":     self.dxy_price,
                "gold_bid":      self.gold_bid,
                "gold_ask":      self.gold_ask,
                "gold_change":   self.gold_change,
                "gold_pct":      self.gold_pct,
                "dxy_change":    self.dxy_change,
                "correlation":   self.correlation,
                "corr_history":  self.corr_history[-100:],
                "signal":        dict(self.current_signal),
                "signals":       self.signals[-50:],
                "bot_logs":      list(self.bot_logs)[-50:],
                "mtf_analysis":  dict(self.mtf_analysis),
                "ohlcv":         {k: v for k, v in self.ohlcv.items()},
                "zones":         dict(self.zones),
                "winrate":       self.winrate,
                "wins":          self.wins,
                "losses":        self.losses,
                "bot_status":    self.bot_status,
                "mt5_connected": self.mt5_connected,
                "gold_symbol":   self.gold_symbol,
                "dxy_symbol":    self.dxy_symbol,
                "last_update":   self.last_update,
                "server_time":   datetime.now().isoformat(),
                "bot_data_received": self.bot_data_received,
            }


STATE = GlobalState()

# ─────────────────────────────────────────────────────────────────────────────
#  CALCULS TECHNIQUES
# ─────────────────────────────────────────────────────────────────────────────

def compute_zones(candles: List[Dict]) -> Dict:
    if not candles or len(candles) < 20:
        return STATE.zones
    try:
        df     = pd.DataFrame(candles)
        highs  = df["high"].values
        lows   = df["low"].values
        closes = df["close"].values
        opens  = df["open"].values
        n      = len(df)

        # ATR
        tr = np.array([max(highs[i]-lows[i],
                           abs(highs[i]-closes[i-1]) if i > 0 else 0,
                           abs(lows[i] -closes[i-1]) if i > 0 else 0)
                       for i in range(n)])
        atr     = float(np.mean(tr[-14:])) if n >= 14 else float(np.mean(tr))
        fvg_min = atr * 0.3

        # S/R
        recent     = df.tail(30)
        support    = round(float(recent["low"].min()),  2)
        resistance = round(float(recent["high"].max()), 2)

        # Swings
        sw_lows, sw_highs, lb = [], [], 3
        for i in range(lb, n-lb):
            if all(lows[i]  < lows[i-j]  for j in range(1,lb+1)) and \
               all(lows[i]  < lows[i+j]  for j in range(1,lb+1)):
                sw_lows.append(round(float(lows[i]), 2))
            if all(highs[i] > highs[i-j] for j in range(1,lb+1)) and \
               all(highs[i] > highs[i+j] for j in range(1,lb+1)):
                sw_highs.append(round(float(highs[i]), 2))

        # FVG
        fvg_bull, fvg_bear = [], []
        for i in range(2, n):
            gb = lows[i] - highs[i-2]
            if gb > fvg_min:
                fvg_bull.append({"low": round(float(highs[i-2]),2),
                                  "high": round(float(lows[i]),2),
                                  "mid": round(float((highs[i-2]+lows[i])/2),2),
                                  "size": round(float(gb),3)})
            gb2 = lows[i-2] - highs[i]
            if gb2 > fvg_min:
                fvg_bear.append({"low": round(float(highs[i]),2),
                                  "high": round(float(lows[i-2]),2),
                                  "mid": round(float((highs[i]+lows[i-2])/2),2),
                                  "size": round(float(gb2),3)})

        # Order Blocks
        ob_buy = ob_sell = None
        for i in range(1, min(n-1, 25)):
            move = abs(closes[i+1]-closes[i])
            if opens[i] > closes[i] and closes[i+1] > opens[i+1] and move > atr*0.5:
                ob_buy  = round(float(lows[i]), 2)
            if closes[i] > opens[i] and closes[i+1] < opens[i+1] and move > atr*0.5:
                ob_sell = round(float(highs[i]), 2)

        return {
            "support": support, "resistance": resistance,
            "swing_lows": sw_lows[-5:], "swing_highs": sw_highs[-5:],
            "fvg_bullish": fvg_bull[-4:], "fvg_bearish": fvg_bear[-4:],
            "ob_buy": ob_buy, "ob_sell": ob_sell,
            "atr": round(atr, 3), "fvg_filter": round(fvg_min, 3),
        }
    except Exception as e:
        log.error(f"compute_zones: {e}")
        return STATE.zones

# ─────────────────────────────────────────────────────────────────────────────
#  MT5 DATA THREAD (fallback si bot non connecté)
# ─────────────────────────────────────────────────────────────────────────────

def _find_symbol(candidates, available):
    for s in candidates:
        if s in available: return s
    for s in available:
        for c in candidates:
            if c.upper() in s.upper(): return s
    return None


def _simulate_ohlcv(symbol: str, n: int, interval_min: int) -> List[Dict]:
    np.random.seed(hash(symbol) % 9999)
    base = 2320.0 if "XAU" in symbol.upper() else 104.5
    vol  = 0.0006 if "XAU" in symbol.upper() else 0.0003
    closes = [base]
    for _ in range(n-1):
        closes.append(closes[-1] * (1 + np.random.normal(0, vol)))
    np.random.seed(int(time.time()*2) % 10000)
    closes[-1] *= (1 + np.random.normal(0, vol*0.3))
    result = []; now = datetime.now()
    for i in range(n):
        t = now - timedelta(minutes=interval_min*(n-1-i))
        o = closes[i-1] if i > 0 else closes[i]; c = closes[i]
        rng2 = abs(c-o)*(1+abs(np.random.normal(0, 0.4)))
        h = max(o,c)+rng2*0.35; l = min(o,c)-rng2*0.35
        result.append({"time": t.isoformat(), "open": round(o,5),
                        "high": round(h,5), "low": round(max(l,0.1),5),
                        "close": round(c,5), "volume": int(np.random.exponential(2000))})
    return result


def _rolling_corr(g: List[float], d: List[float], w: int = CORR_WINDOW) -> float:
    n = min(len(g), len(d), w)
    if n < 5: return -0.75
    ga, da = np.array(g[-n:]), np.array(d[-n:])
    if np.std(ga) == 0 or np.std(da) == 0: return 0.0
    return float(np.corrcoef(ga, da)[0, 1])


def _compute_signal(gc: List[float], dc: List[float], corr: float) -> Dict:
    if len(gc) < 3 or len(dc) < 3:
        return {"direction": "WAIT", "anticipation": None, "confidence": 0}
    gold_up = gc[-1] > gc[-2]; dxy_down = dc[-1] < dc[-2]; dxy_up = dc[-1] > dc[-2]
    direction = "WAIT"; confidence = 0; anticipation = None
    if corr < -0.60:
        if gold_up and dxy_down:
            direction = "BUY";  confidence = min(100, int(abs(corr)*100))
        elif not gold_up and dxy_up:
            direction = "SELL"; confidence = min(100, int(abs(corr)*100))
    dxy_move = abs(dc[-1]-dc[-3]) / (abs(dc[-3])+1e-9)
    if direction == "WAIT" and dxy_move > 0.0002:
        anticipation = "ANTICIPATION BUY GOLD" if dxy_down else "ANTICIPATION SELL GOLD"
    return {"direction": direction, "anticipation": anticipation, "confidence": confidence}


def _trend(closes: List[float]) -> str:
    if len(closes) < 6: return "→ Stable"
    pct = (closes[-1]-closes[-5]) / (closes[-5]+1e-9) * 100
    return "↑ Hausse" if pct > 0.05 else ("↓ Baisse" if pct < -0.05 else "→ Stable")


def mt5_data_thread():
    """
    Thread de fallback MT5 / simulation.
    Ne s'exécute en mode actif que si le bot n'a pas encore envoyé de données.
    Si le bot est connecté et envoie des snapshots, ce thread ne met à jour
    que les prix tick (haute fréquence) et laisse le reste au bot.
    """
    if MT5_AVAILABLE:
        ok = mt5.initialize()
        if ok:
            available = {s.name for s in mt5.symbols_get()}
            STATE.gold_symbol   = _find_symbol(GOLD_SYMBOLS, available)
            STATE.dxy_symbol    = _find_symbol(DXY_SYMBOLS,  available)
            STATE.mt5_connected = True
            STATE.bot_status    = "running"
            STATE.add_log("INFO", f"MT5 | Gold:{STATE.gold_symbol} DXY:{STATE.dxy_symbol}")
        else:
            STATE.bot_status = "simulation"
            STATE.add_log("WARNING", "MT5 non connecté — simulation")
    else:
        STATE.bot_status = "simulation"
        STATE.add_log("WARNING", "MetaTrader5 non installé — simulation")

    MT5_TF = {}
    if MT5_AVAILABLE and STATE.mt5_connected:
        MT5_TF = {"M5": mt5.TIMEFRAME_M5, "M15": mt5.TIMEFRAME_M15, "H1": mt5.TIMEFRAME_H1}

    sim_gold, sim_dxy = 2320.0, 104.5
    ohlcv_tick = 0

    while True:
        try:
            # ── Prix tick (toujours actif) ────────────────────────────────────
            if MT5_AVAILABLE and STATE.mt5_connected and STATE.gold_symbol:
                tg = mt5.symbol_info_tick(STATE.gold_symbol)
                td = mt5.symbol_info_tick(STATE.dxy_symbol) if STATE.dxy_symbol else None
                gold_p = float(tg.bid) if tg else sim_gold
                dxy_p  = float(td.bid) if td else sim_dxy
                gold_b = float(tg.bid) if tg else gold_p
                gold_a = float(tg.ask) if tg else gold_p + 0.3
            else:
                sim_gold += np.random.normal(0, 0.04)
                sim_dxy  += np.random.normal(0, 0.002)
                gold_p = round(sim_gold, 2); dxy_p = round(sim_dxy, 3)
                gold_b = gold_p; gold_a = gold_p + 0.25

            with STATE._lock:
                prev_g = STATE.gold_price or gold_p
                prev_d = STATE.dxy_price  or dxy_p
                STATE.gold_prev   = prev_g
                STATE.dxy_prev    = prev_d
                STATE.gold_price  = gold_p
                STATE.dxy_price   = dxy_p
                STATE.gold_bid    = gold_b
                STATE.gold_ask    = gold_a
                STATE.gold_change = round(gold_p - prev_g, 2)
                STATE.gold_pct    = round((gold_p - prev_g) / prev_g * 100, 3) if prev_g else 0.0
                STATE.dxy_change  = round(dxy_p - prev_d, 4)
                STATE.last_update = datetime.now().isoformat()

            # ── OHLCV + Zones + MTF — seulement si bot n'a pas envoyé de données ──
            ohlcv_tick += 1
            if ohlcv_tick >= 5 and not STATE.bot_data_received:
                ohlcv_tick = 0
                TF_MAP = {"M5": 5, "M15": 15, "H1": 60}
                for tf_name, tf_min in TF_MAP.items():
                    n_bars = 200 if tf_name == "M5" else (150 if tf_name == "M15" else 100)
                    if MT5_AVAILABLE and STATE.mt5_connected and STATE.gold_symbol:
                        rates = mt5.copy_rates_from_pos(
                            STATE.gold_symbol, MT5_TF[tf_name], 0, n_bars)
                        if rates is not None and len(rates):
                            df = pd.DataFrame(rates)
                            df["time"] = pd.to_datetime(df["time"], unit="s")
                            candles = [{"time": r["time"].isoformat(),
                                        "open": round(r["open"],5), "high": round(r["high"],5),
                                        "low": round(r["low"],5), "close": round(r["close"],5),
                                        "volume": int(r.get("tick_volume",0))}
                                       for _,r in df.iterrows()]
                        else:
                            candles = _simulate_ohlcv("XAUUSD", n_bars, tf_min)
                    else:
                        candles = _simulate_ohlcv("XAUUSD", n_bars, tf_min)

                    with STATE._lock:
                        STATE.ohlcv[tf_name] = candles

                    gc      = [c["close"] for c in candles]
                    dc      = [c["close"] for c in _simulate_ohlcv("DXY", n_bars, tf_min)]
                    tf_corr = _rolling_corr(gc, dc)
                    tf_sig  = _compute_signal(gc, dc, tf_corr)
                    with STATE._lock:
                        STATE.mtf_analysis[tf_name] = {
                            "signal": tf_sig["direction"], "corr": round(tf_corr,4),
                            "trend": _trend(gc), "anticipation": tf_sig.get("anticipation"),
                        }

                with STATE._lock:
                    m5c = STATE.ohlcv.get("M5", [])
                if m5c:
                    z = compute_zones(m5c)
                    with STATE._lock:
                        STATE.zones = z

            elif ohlcv_tick >= 5:
                ohlcv_tick = 0   # reset le compteur même si bot actif

            # Corrélation globale (seulement si pas de données bot)
            if not STATE.bot_data_received:
                with STATE._lock:
                    gc = [c["close"] for c in STATE.ohlcv.get("M5", [])]
                if gc:
                    dc   = [c["close"] for c in _simulate_ohlcv("DXY", 100, 5)]
                    corr = _rolling_corr(gc, dc)
                    sig  = _compute_signal(gc, dc, corr)
                    with STATE._lock:
                        STATE.correlation = corr
                        STATE.corr_history.append({"time": datetime.now().isoformat(),
                                                   "corr": round(corr,4)})
                        if len(STATE.corr_history) > 500:
                            STATE.corr_history = STATE.corr_history[-500:]
                        STATE.current_signal.update({
                            "direction": sig["direction"],
                            "anticipation": sig.get("anticipation"),
                            "confidence": sig["confidence"],
                            "corr": round(corr,4),
                            "gold_price": round(gold_p,2),
                            "dxy_price":  round(dxy_p,3),
                            "time": datetime.now().isoformat(),
                        })

            time.sleep(0.5)

        except Exception as e:
            log.error(f"Data thread: {e}", exc_info=True)
            STATE.add_log("ERROR", str(e))
            time.sleep(2)

# ─────────────────────────────────────────────────────────────────────────────
#  BROADCAST LOOP
# ─────────────────────────────────────────────────────────────────────────────

async def broadcast_loop():
    last_price = last_ohlcv = last_zones = last_logs = 0.0
    while True:
        now = time.time()
        if ws_manager.count == 0:
            await asyncio.sleep(0.3); continue

        if now - last_price >= WS_PRICE_INTERVAL:
            await ws_manager.broadcast(STATE.price_frame())
            last_price = now

        if STATE.signal_changed():
            await ws_manager.broadcast(STATE.signal_frame())

        if now - last_ohlcv >= WS_OHLCV_INTERVAL:
            for tf in ["M5","M15","H1"]:
                await ws_manager.broadcast(STATE.ohlcv_frame(tf))
            last_ohlcv = now

        if now - last_zones >= WS_ZONES_INTERVAL:
            await ws_manager.broadcast(STATE.zones_frame())
            last_zones = now

        if now - last_logs >= 5.0:
            await ws_manager.broadcast(STATE.logs_frame())
            last_logs = now

        await asyncio.sleep(0.1)

# ─────────────────────────────────────────────────────────────────────────────
#  APP
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Gold/DXY API v3.1", version="3.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


def _auth(request: Request):
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

# ── WebSocket ──────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, api_key: str = Query(default="")):
    if api_key != API_KEY:
        await websocket.close(code=4001, reason="Unauthorized"); return
    await ws_manager.connect(websocket)
    try:
        await ws_manager.send_to(websocket, STATE.full_snapshot())
        while True:
            try:
                msg  = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                data = json.loads(msg)
                if data.get("cmd") == "ping":
                    await ws_manager.send_to(websocket, {"type":"pong","ts":datetime.now().isoformat()})
                elif data.get("cmd") == "get_ohlcv":
                    await ws_manager.send_to(websocket, STATE.ohlcv_frame(data.get("tf","M5")))
                elif data.get("cmd") == "get_snapshot":
                    await ws_manager.send_to(websocket, STATE.full_snapshot())
            except asyncio.TimeoutError:
                await ws_manager.send_to(websocket, {"type":"ping","ts":datetime.now().isoformat()})
    except WebSocketDisconnect:
        pass
    finally:
        await ws_manager.disconnect(websocket)

# ── HTTP endpoints ─────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"service": "Gold/DXY API v3.1", "ws_clients": ws_manager.count,
            "status": STATE.bot_status, "bot_data": STATE.bot_data_received}

@app.get("/health")
def health():
    return {"status": "ok", "mt5": STATE.mt5_connected,
            "ws_clients": ws_manager.count, "bot_data": STATE.bot_data_received,
            "time": datetime.now().isoformat()}

@app.get("/api/snapshot")
def get_snapshot(request: Request):
    _auth(request); return JSONResponse(STATE.full_snapshot())

@app.get("/api/ohlcv/{timeframe}")
def get_ohlcv(timeframe: str, request: Request):
    _auth(request)
    tf = timeframe.upper()
    if tf not in ("M5","M15","H1"): raise HTTPException(400, f"TF invalide: {tf}")
    return STATE.ohlcv_frame(tf)

@app.get("/api/zones")
def get_zones(request: Request):
    _auth(request); return STATE.zones_frame()

@app.get("/api/stats")
def get_stats(request: Request):
    _auth(request)
    with STATE._lock:
        return {"winrate": STATE.winrate, "wins": STATE.wins, "losses": STATE.losses,
                "total": len(STATE.signals), "status": STATE.bot_status,
                "mt5": STATE.mt5_connected, "ws_clients": ws_manager.count,
                "bot_data": STATE.bot_data_received}


# ── PUSH BOT → API ─────────────────────────────────────────────────────────────

class SnapshotPushPayload(BaseModel):
    """Payload complet envoyé par le bot à chaque cycle."""
    gold_price:    float = 0.0
    dxy_price:     float = 0.0
    gold_bid:      float = 0.0
    gold_ask:      float = 0.0
    gold_change:   float = 0.0
    gold_pct:      float = 0.0
    dxy_change:    float = 0.0
    correlation:   float = 0.0
    signal:        Optional[Dict[str, Any]] = None
    mtf:           Optional[Dict[str, Any]] = None
    ohlcv:         Optional[Dict[str, Any]] = None
    signals:       Optional[List[Dict]]     = None
    winrate:       float = 0.0
    wins:          int   = 0
    losses:        int   = 0
    bot_status:    str   = "running"
    mt5_connected: bool  = False
    gold_symbol:   Optional[str] = None
    last_update:   Optional[str] = None


@app.post("/api/snapshot/push")
async def push_snapshot(payload: SnapshotPushPayload, request: Request):
    """
    Endpoint appelé par le bot à chaque cycle pour mettre à jour le STATE.
    Remplace totalement la logique WebSocket du bot.
    """
    _auth(request)
    data = payload.dict()
    STATE.apply_bot_snapshot(data)

    # Recalcul zones si OHLCV M5 reçu
    if payload.ohlcv and payload.ohlcv.get("M5"):
        z = compute_zones(payload.ohlcv["M5"])
        with STATE._lock:
            STATE.zones = z

    # Log
    STATE.add_log("INFO", f"Snapshot reçu | Gold={payload.gold_price:.2f} "
                           f"Corr={payload.correlation:+.3f} "
                           f"Bot={payload.bot_status}")

    # Broadcast WS si signal actif
    if payload.signal and payload.signal.get("direction") in ("BUY","SELL"):
        await ws_manager.broadcast(STATE.signal_frame())

    return {"status": "ok", "received": datetime.now().isoformat()}


class SignalPayload(BaseModel):
    direction:      str;   tf:         str;  entry:    float
    tp:             float; sl:         float; rr:       float
    sl_source:      str   = "ATR"
    corr:           float = 0.0
    confidence:     int   = 0
    anticipation:   Optional[str] = None
    pipeline_state: str   = "IDLE"
    result:         str   = "OPEN"
    time:           Optional[str] = None


@app.post("/api/signal/push")
async def push_signal(payload: SignalPayload, request: Request):
    _auth(request)
    data = payload.dict()
    data["time"] = data.get("time") or datetime.now().isoformat()
    STATE.set_signal(data)
    if payload.direction in ("BUY","SELL"):
        STATE.add_signal_to_history(data)
    STATE.add_log("SIGNAL", f"{payload.direction} {payload.tf} @ {payload.entry} "
                             f"TP={payload.tp} SL={payload.sl} R/R=1:{payload.rr}")
    await ws_manager.broadcast(STATE.signal_frame())
    return {"status": "ok"}


class LogPayload(BaseModel):
    level: str = "INFO"; msg: str

@app.post("/api/log/push")
def push_log(payload: LogPayload, request: Request):
    _auth(request); STATE.add_log(payload.level, payload.msg); return {"status": "ok"}


class ResultPayload(BaseModel):
    signal_id: int; result: str

@app.post("/api/signal/result")
def update_result(payload: ResultPayload, request: Request):
    _auth(request)
    with STATE._lock:
        for s in STATE.signals:
            if s.get("id") == payload.signal_id:
                s["result"] = payload.result; break
        STATE._update_winrate(); STATE._save_signals()
    return {"status": "ok"}


@app.on_event("startup")
async def startup_event():
    threading.Thread(target=mt5_data_thread, daemon=True).start()
    asyncio.create_task(broadcast_loop())
    log.info(f"API v3.1 démarrée | HTTP+WS :{API_PORT}")
    log.info(f"Snapshot push : POST /api/snapshot/push")
    log.info(f"Signal push   : POST /api/signal/push")
    log.info(f"Snapshot GET  : GET  /api/snapshot")


if __name__ == "__main__":
    uvicorn.run("api_server:app", host=API_HOST, port=API_PORT,
                reload=False, log_level="info")
