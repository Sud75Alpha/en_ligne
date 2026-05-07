"""
API SERVER v4.0 — GOLD/DXY
- POST /api/snapshot/push  ← bot envoie données
- POST /api/signal/push    ← bot envoie signal
- POST /api/signal/result  ← mise à jour WIN/LOSS
- POST /api/backtest/push  ← backtest envoie résultats
- GET  /api/snapshot       ← dashboard lit
"""

import os, json, time, asyncio, logging, threading
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Set, Any
from collections import deque

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

API_HOST = "0.0.0.0"
API_PORT = int(os.getenv("API_PORT", 8000))
API_KEY  = os.getenv("API_KEY", "gold_dxy_secret_2024")
MAX_SIGNALS = 200

logging.basicConfig(level=logging.INFO,
    format="[%(asctime)s] %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("API")

# ── WEBSOCKET ──────────────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.active.add(ws)

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

# ── STATE ──────────────────────────────────────────────────────────────────────
class GlobalState:
    def __init__(self):
        self._lock = threading.RLock()
        self.gold_price:   float = 0.0
        self.dxy_price:    float = 0.0
        self.gold_bid:     float = 0.0
        self.gold_ask:     float = 0.0
        self.gold_change:  float = 0.0
        self.gold_pct:     float = 0.0
        self.dxy_change:   float = 0.0
        self.correlation:  float = -0.75
        self.current_signal: Dict = {
            "direction": "WAIT", "anticipation": None, "confidence": 0,
            "corr": -0.75, "gold_price": 0.0, "dxy_price": 0.0,
            "entry": 0.0, "tp": 0.0, "sl": 0.0, "rr": 0.0,
            "lot": 0.0, "sl_source": "—", "pipeline_state": "IDLE",
        }
        self._last_sig_hash = ""
        self.signals: List[Dict] = []
        self._load_signals()
        self.bot_logs: deque = deque(maxlen=300)
        self.bot_logs.append({"time": datetime.now().strftime("%H:%M:%S"),
                               "level": "INFO", "msg": "API v4.0 démarrée"})
        self.ohlcv: Dict[str, List] = {"M5": [], "M15": [], "H1": []}
        self.zones: Dict = {"support":0.0,"resistance":0.0,"fvg_bullish":[],
                             "fvg_bearish":[],"ob_buy":None,"ob_sell":None,"atr":0.0,"fvg_filter":0.0}
        self.mtf: Dict = {
            "H1":  {"signal":"WAIT","corr":0.0,"trend":"—","anticipation":None},
            "M15": {"signal":"WAIT","corr":0.0,"trend":"—","anticipation":None},
            "M5":  {"signal":"WAIT","corr":0.0,"trend":"—","anticipation":None},
        }
        self.winrate: float = 0.0
        self.wins:    int   = 0
        self.losses:  int   = 0
        self.bot_status:    str  = "waiting_bot"
        self.mt5_connected: bool = False
        self.gold_symbol:   Optional[str] = None
        self.last_update:   str  = datetime.now().isoformat()
        self.bot_data_received: bool = False
        # Backtest
        self.backtest_stats:  Dict = {}
        self.backtest_trades: List = []
        self.backtest_equity: List = []
        self.backtest_patterns: Dict = {}

    def add_log(self, level: str, msg: str):
        with self._lock:
            self.bot_logs.append({"time": datetime.now().strftime("%H:%M:%S"),
                                   "level": level, "msg": msg})

    def signal_changed(self) -> bool:
        h = f"{self.current_signal.get('direction')}{self.current_signal.get('confidence')}{self.current_signal.get('entry')}"
        if h != self._last_sig_hash:
            self._last_sig_hash = h; return True
        return False

    def apply_bot_snapshot(self, data: Dict):
        with self._lock:
            for k in ["gold_price","dxy_price","gold_bid","gold_ask","gold_change",
                       "gold_pct","dxy_change","correlation","bot_status",
                       "mt5_connected","gold_symbol","last_update"]:
                if k in data: setattr(self, k, data[k])
            if "signal" in data and isinstance(data["signal"], dict):
                self.current_signal = {**self.current_signal, **data["signal"],
                                       "time": datetime.now().isoformat()}
            if "mtf" in data and isinstance(data["mtf"], dict):
                for tf_n, tf_d in data["mtf"].items():
                    if tf_n in self.mtf and isinstance(tf_d, dict):
                        self.mtf[tf_n] = {**self.mtf[tf_n], **tf_d}
            if "ohlcv" in data and isinstance(data["ohlcv"], dict):
                for tf_n, candles in data["ohlcv"].items():
                    if candles and isinstance(candles, list):
                        self.ohlcv[tf_n] = candles
            if "signals" in data and isinstance(data["signals"], list):
                existing = {s.get("time") for s in self.signals}
                for s in data["signals"]:
                    if s.get("time") not in existing:
                        self.signals.append(s)
                self.signals = self.signals[-MAX_SIGNALS:]
            for k in ["winrate","wins","losses"]:
                if k in data: setattr(self, k, data[k])
            self.last_update = datetime.now().isoformat()
            self.bot_data_received = True
            if self.ohlcv.get("M5") and not self.zones.get("atr"):
                self.zones = compute_zones(self.ohlcv["M5"])

    def add_signal(self, signal: Dict):
        with self._lock:
            signal["id"]   = int(time.time() * 1000)
            signal["time"] = signal.get("time") or datetime.now().isoformat()
            self.signals.append(signal)
            if len(self.signals) > MAX_SIGNALS:
                self.signals = self.signals[-MAX_SIGNALS:]
            self._save_signals()
            self._update_stats()

    def update_signal_result(self, signal_id: int, result: str):
        with self._lock:
            for s in self.signals:
                if s.get("id") == signal_id:
                    s["result"] = result; break
            self._update_stats()
            self._save_signals()

    def update_last_open_result(self, result: str):
        """Met à jour le dernier signal OPEN avec WIN ou LOSS."""
        with self._lock:
            for s in reversed(self.signals):
                if s.get("result") == "OPEN":
                    s["result"] = result; break
            self._update_stats()
            self._save_signals()

    def _update_stats(self):
        closed = [s for s in self.signals if s.get("result") in ("WIN","LOSS")]
        if closed:
            self.wins    = sum(1 for s in closed if s["result"]=="WIN")
            self.losses  = len(closed) - self.wins
            self.winrate = round(self.wins / len(closed) * 100, 1)

    def _load_signals(self):
        try:
            if os.path.exists("signals.json"):
                with open("signals.json") as f:
                    self.signals = json.load(f)
                self._update_stats()
        except Exception:
            self.signals = []

    def _save_signals(self):
        try:
            with open("signals.json","w") as f:
                json.dump(self.signals, f, indent=2, default=str)
        except Exception:
            pass

    def full_snapshot(self) -> Dict:
        with self._lock:
            return {
                "type":           "snapshot",
                "gold_price":     self.gold_price,
                "dxy_price":      self.dxy_price,
                "gold_bid":       self.gold_bid,
                "gold_ask":       self.gold_ask,
                "gold_change":    self.gold_change,
                "gold_pct":       self.gold_pct,
                "dxy_change":     self.dxy_change,
                "correlation":    self.correlation,
                "signal":         dict(self.current_signal),
                "signals":        self.signals[-50:],
                "bot_logs":       list(self.bot_logs)[-50:],
                "mtf_analysis":   dict(self.mtf),
                "ohlcv":          {k: v for k, v in self.ohlcv.items()},
                "zones":          dict(self.zones),
                "winrate":        self.winrate,
                "wins":           self.wins,
                "losses":         self.losses,
                "bot_status":     self.bot_status,
                "mt5_connected":  self.mt5_connected,
                "gold_symbol":    self.gold_symbol,
                "last_update":    self.last_update,
                "bot_data_received": self.bot_data_received,
                "backtest_stats":    self.backtest_stats,
                "backtest_trades":   self.backtest_trades[-100:],
                "backtest_equity":   self.backtest_equity,
                "backtest_patterns": self.backtest_patterns,
                "server_time":    datetime.now().isoformat(),
            }

STATE = GlobalState()

# ── ZONES ──────────────────────────────────────────────────────────────────────
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
        tr = np.array([max(highs[i]-lows[i],
                           abs(highs[i]-closes[i-1]) if i>0 else 0,
                           abs(lows[i] -closes[i-1]) if i>0 else 0)
                       for i in range(n)])
        atr     = float(np.mean(tr[-14:])) if n>=14 else float(np.mean(tr))
        fvg_min = atr * 0.3
        recent  = df.tail(30)
        support    = round(float(recent["low"].min()), 2)
        resistance = round(float(recent["high"].max()), 2)
        sw_lows, sw_highs, lb = [], [], 3
        for i in range(lb, n-lb):
            if all(lows[i]<lows[i-j]  for j in range(1,lb+1)) and \
               all(lows[i]<lows[i+j]  for j in range(1,lb+1)):
                sw_lows.append(round(float(lows[i]),2))
            if all(highs[i]>highs[i-j] for j in range(1,lb+1)) and \
               all(highs[i]>highs[i+j] for j in range(1,lb+1)):
                sw_highs.append(round(float(highs[i]),2))
        fvg_bull, fvg_bear = [], []
        for i in range(2,n):
            gb = lows[i]-highs[i-2]
            if gb > fvg_min:
                fvg_bull.append({"low":round(float(highs[i-2]),2),"high":round(float(lows[i]),2),
                                  "mid":round(float((highs[i-2]+lows[i])/2),2)})
            gb2 = lows[i-2]-highs[i]
            if gb2 > fvg_min:
                fvg_bear.append({"low":round(float(highs[i]),2),"high":round(float(lows[i-2]),2),
                                  "mid":round(float((highs[i]+lows[i-2])/2),2)})
        ob_buy = ob_sell = None
        for i in range(1, min(n-1,25)):
            move = abs(closes[i+1]-closes[i])
            if opens[i]>closes[i] and closes[i+1]>opens[i+1] and move>atr*0.5:
                ob_buy  = round(float(lows[i]),2)
            if closes[i]>opens[i] and closes[i+1]<opens[i+1] and move>atr*0.5:
                ob_sell = round(float(highs[i]),2)
        return {"support":support,"resistance":resistance,
                "swing_lows":sw_lows[-5:],"swing_highs":sw_highs[-5:],
                "fvg_bullish":fvg_bull[-4:],"fvg_bearish":fvg_bear[-4:],
                "ob_buy":ob_buy,"ob_sell":ob_sell,
                "atr":round(atr,3),"fvg_filter":round(fvg_min,3)}
    except Exception as e:
        log.error(f"compute_zones: {e}")
        return STATE.zones

# ── BROADCAST ──────────────────────────────────────────────────────────────────
async def broadcast_loop():
    last_price = last_ohlcv = last_logs = 0.0
    while True:
        now = time.time()
        if ws_manager.count == 0:
            await asyncio.sleep(0.3); continue
        if now - last_price >= 0.5:
            await ws_manager.broadcast({"type":"price",
                "gold": STATE.gold_price, "dxy": STATE.dxy_price,
                "gold_change": STATE.gold_change, "gold_pct": STATE.gold_pct,
                "dxy_change": STATE.dxy_change, "corr": STATE.correlation,
                "gold_bid": STATE.gold_bid, "gold_ask": STATE.gold_ask})
            last_price = now
        if STATE.signal_changed():
            await ws_manager.broadcast({"type":"signal",
                "signal": STATE.current_signal, "mtf": STATE.mtf,
                "stats": {"winrate":STATE.winrate,"wins":STATE.wins,"losses":STATE.losses}})
        if now - last_ohlcv >= 8.0:
            for tf in ["M5","M15","H1"]:
                await ws_manager.broadcast({"type":"ohlcv","timeframe":tf,
                    "candles":STATE.ohlcv.get(tf,[])})
            last_ohlcv = now
        if now - last_logs >= 5.0:
            await ws_manager.broadcast({"type":"logs",
                "logs":list(STATE.bot_logs)[-30:]})
            last_logs = now
        await asyncio.sleep(0.1)

# ── APP ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Gold/DXY API v4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

def _auth(request: Request):
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if key != API_KEY:
        raise HTTPException(401, "Invalid API key")

# ── ENDPOINTS ──────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"service": "Gold/DXY API v4.0", "status": STATE.bot_status,
            "bot_data": STATE.bot_data_received, "ws_clients": ws_manager.count}

@app.get("/health")
def health():
    return {"status": "ok", "mt5": STATE.mt5_connected,
            "bot_data": STATE.bot_data_received,
            "gold_price": STATE.gold_price, "bot_status": STATE.bot_status,
            "ws_clients": ws_manager.count, "time": datetime.now().isoformat()}

@app.get("/api/snapshot")
def get_snapshot(request: Request):
    _auth(request)
    return JSONResponse(STATE.full_snapshot())

@app.get("/api/stats")
def get_stats(request: Request):
    _auth(request)
    return {"winrate": STATE.winrate, "wins": STATE.wins, "losses": STATE.losses,
            "total": len(STATE.signals), "bot_data": STATE.bot_data_received}

@app.get("/api/backtest")
def get_backtest(request: Request):
    _auth(request)
    return {"stats": STATE.backtest_stats, "trades": STATE.backtest_trades,
            "equity_curve": STATE.backtest_equity, "patterns": STATE.backtest_patterns}

# ── PUSH SNAPSHOT (bot → API) ──────────────────────────────────────────────────
class SnapshotPayload(BaseModel):
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
async def push_snapshot(payload: SnapshotPayload, request: Request):
    _auth(request)
    STATE.apply_bot_snapshot(payload.dict())
    if payload.ohlcv and payload.ohlcv.get("M5"):
        z = compute_zones(payload.ohlcv["M5"])
        with STATE._lock:
            STATE.zones = z
    STATE.add_log("INFO", f"Snapshot | Gold={payload.gold_price:.2f} "
                           f"Corr={payload.correlation:+.3f}")
    if payload.signal and payload.signal.get("direction") in ("BUY","SELL"):
        await ws_manager.broadcast({"type":"signal",
            "signal": STATE.current_signal, "mtf": STATE.mtf})
    return {"status": "ok"}

# ── PUSH SIGNAL ────────────────────────────────────────────────────────────────
class SignalPayload(BaseModel):
    direction:      str
    tf:             str
    entry:          float
    tp:             float
    sl:             float
    rr:             float
    lot:            float  = 0.01
    sl_source:      str    = "ATR"
    corr:           float  = 0.0
    confidence:     int    = 0
    anticipation:   Optional[str] = None
    pipeline_state: str    = "IDLE"
    result:         str    = "OPEN"
    time:           Optional[str] = None

@app.post("/api/signal/push")
async def push_signal(payload: SignalPayload, request: Request):
    _auth(request)
    data = payload.dict()
    data["time"] = data.get("time") or datetime.now().isoformat()
    with STATE._lock:
        STATE.current_signal = {**STATE.current_signal, **data}
    if payload.direction in ("BUY","SELL"):
        STATE.add_signal(data)
    STATE.add_log("SIGNAL", f"{payload.direction} {payload.tf} @ {payload.entry} "
                             f"TP={payload.tp} SL={payload.sl} R/R=1:{payload.rr} "
                             f"Lot={payload.lot}")
    await ws_manager.broadcast({"type":"signal",
        "signal": STATE.current_signal, "mtf": STATE.mtf,
        "stats": {"winrate":STATE.winrate,"wins":STATE.wins,"losses":STATE.losses}})
    return {"status": "ok"}

# ── UPDATE RESULT ──────────────────────────────────────────────────────────────
class ResultPayload(BaseModel):
    signal_id: Optional[int] = None
    result:    str   # WIN ou LOSS

@app.post("/api/signal/result")
async def update_result(payload: ResultPayload, request: Request):
    _auth(request)
    if payload.signal_id:
        STATE.update_signal_result(payload.signal_id, payload.result)
    else:
        STATE.update_last_open_result(payload.result)

    # Met aussi à jour current_signal pour que le dashboard reflète le résultat
    with STATE._lock:
        STATE.current_signal["result"]         = payload.result
        STATE.current_signal["pipeline_state"] = "IDLE"
        if payload.result in ("WIN", "LOSS"):
            STATE.current_signal["direction"] = "WAIT"

    STATE.add_log("INFO", f"Trade clôturé → {payload.result} | "
                           f"WR={STATE.winrate}% ({STATE.wins}W/{STATE.losses}L)")

    await ws_manager.broadcast({"type": "signal",
        "signal": STATE.current_signal, "mtf": STATE.mtf,
        "stats": {"winrate": STATE.winrate, "wins": STATE.wins,
                  "losses": STATE.losses, "total": len(STATE.signals)}})
    return {"status": "ok", "winrate": STATE.winrate}

# ── PUSH BACKTEST ──────────────────────────────────────────────────────────────
class BacktestPayload(BaseModel):
    stats:        Dict[str, Any]  = {}
    trades:       List[Dict]      = []
    equity_curve: List[Dict]      = []
    patterns:     Dict[str, Any]  = {}

@app.post("/api/backtest/push")
async def push_backtest(payload: BacktestPayload, request: Request):
    _auth(request)
    with STATE._lock:
        STATE.backtest_stats    = payload.stats
        STATE.backtest_trades   = payload.trades
        STATE.backtest_equity   = payload.equity_curve
        STATE.backtest_patterns = payload.patterns
    STATE.add_log("INFO", f"Backtest reçu | {payload.stats.get('total',0)} trades "
                           f"| WR={payload.stats.get('winrate',0)}% "
                           f"| PF={payload.stats.get('profit_factor',0)}")
    try:
        with open("backtest_results.json","w") as f:
            json.dump(payload.dict(), f, indent=2, default=str)
    except Exception:
        pass
    return {"status": "ok"}

@app.get("/api/signals")
def get_signals(request: Request):
    _auth(request)
    with STATE._lock:
        return {
            "signals":  STATE.signals[-100:],
            "winrate":  STATE.winrate,
            "wins":     STATE.wins,
            "losses":   STATE.losses,
            "total":    len(STATE.signals),
        }

# ── LOG PUSH ───────────────────────────────────────────────────────────────────
class LogPayload(BaseModel):
    level: str = "INFO"
    msg:   str

@app.post("/api/log/push")
def push_log(payload: LogPayload, request: Request):
    _auth(request)
    STATE.add_log(payload.level, payload.msg)
    return {"status": "ok"}

# ── WEBSOCKET ──────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket, api_key: str = Query(default="")):
    if api_key != API_KEY:
        await websocket.close(code=4001); return
    await ws_manager.connect(websocket)
    try:
        await ws_manager.send_to(websocket, STATE.full_snapshot())
        while True:
            try:
                msg  = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                data = json.loads(msg)
                if data.get("cmd") == "ping":
                    await ws_manager.send_to(websocket, {"type":"pong"})
                elif data.get("cmd") == "get_snapshot":
                    await ws_manager.send_to(websocket, STATE.full_snapshot())
            except asyncio.TimeoutError:
                await ws_manager.send_to(websocket, {"type":"ping"})
    except WebSocketDisconnect:
        pass
    finally:
        await ws_manager.disconnect(websocket)

@app.on_event("startup")
async def startup():
    asyncio.create_task(broadcast_loop())
    log.info(f"API v4.0 démarrée sur :{API_PORT}")
    STATE.bot_status = "waiting_bot"

if __name__ == "__main__":
    uvicorn.run("api_server:app", host=API_HOST, port=API_PORT, reload=False)
