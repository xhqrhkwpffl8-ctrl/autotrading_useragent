"""
AutoTrading User Agent
- 유저 서버에서 실행되는 Agent (Railway 배포)
- 중앙 서버에서 HMAC 서명된 주문 요청을 받아 거래소에 직접 실행
- 시작 시 자동으로 중앙 서버에 URL 등록
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, List, Dict

import httpx
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from config import settings
from exchange_client import AgentExchangeClient, ExchangeError

# ===== 로깅 설정 =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("agent")

# ===== 거래소 클라이언트 싱글톤 =====
_exchange_client: Optional[AgentExchangeClient] = None

# ===== 수동 포지션 감지 상태 =====
_known_positions: Dict[str, Optional[dict]] = {}   # symbol → position dict (None = no position)
_bot_executed_symbols: set = set()                  # 봇이 market_entry 실행한 심볼 (수동 오감지 방지)


def get_exchange_client() -> AgentExchangeClient:
    global _exchange_client
    if _exchange_client is None:
        _exchange_client = AgentExchangeClient(
            exchange_id=settings.exchange,
            api_key=settings.api_key,
            api_secret=settings.api_secret,
            api_passphrase=settings.api_passphrase,
        )
    return _exchange_client


# ===== 보안 유틸 =====

def verify_bearer_token(authorization: str) -> bool:
    """Bearer 토큰 검증"""
    if not authorization or not authorization.startswith("Bearer "):
        return False
    token = authorization[7:]
    return hmac.compare_digest(token, settings.agent_token)


def verify_hmac_signature(payload: dict) -> bool:
    """HMAC-SHA256 서명 검증 (timestamp 필드 제외 후 서명 검증)"""
    sig = payload.pop("hmac_signature", "")
    if not sig:
        return False
    canonical = json.dumps(payload, sort_keys=True)
    expected = hmac.new(
        settings.token_secret.encode(),
        canonical.encode(),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, sig)


def check_timestamp(timestamp_str: str, max_age_seconds: int = 60) -> bool:
    """Timestamp 60초 초과 거부"""
    try:
        ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        diff = abs((now - ts).total_seconds())
        return diff <= max_age_seconds
    except Exception:
        return False


# ===== 스키마 =====

class ExecuteRequest(BaseModel):
    order_type: str             # "market_entry"|"close"|"set_sl"|"set_leverage"|"cancel_order"|"cancel_all"|"adjust"
    symbol: str
    side: Optional[str] = None          # "Buy" or "Sell"
    qty: Optional[str] = None           # 수량 문자열 (정밀도 처리는 거래소 클라이언트에서)
    price: Optional[str] = None         # 지정가 주문 가격 문자열
    sl_price: Optional[float] = None
    tp_orders: Optional[List[dict]] = None   # [{"side": "Sell", "qty": "0.001", "price": "45000.0"}]
    dca_orders: Optional[List[dict]] = None  # DCA 추가매수 지정가 주문 목록 (market_entry와 함께)
    leverage: Optional[int] = None
    order_id: Optional[str] = None
    expected_position_size: Optional[float] = None  # None = 검증 생략
    timestamp: str              # ISO 형식
    hmac_signature: str


# ===== 중앙 서버 등록 =====

async def register_with_central(agent_url: str):
    """중앙 서버에 Agent URL 등록"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{settings.central_url}/api/agent/register",
                json={
                    "agent_url": agent_url,
                    "agent_token": settings.agent_token,
                    "exchange": settings.exchange,
                    "referral_code": settings.referral_code,
                }
            )
            if resp.status_code == 200:
                logger.info(f"Successfully registered with central server: {agent_url}")
            else:
                logger.warning(f"Registration failed: HTTP {resp.status_code} - {resp.text}")
    except Exception as e:
        logger.error(f"Failed to register with central server: {e}")


# ===== 수동 포지션 감지 =====

def _normalize_symbol(raw_symbol: str) -> str:
    """ccxt 심볼 형식 → Bybit 형식 변환 (BTC/USDT:USDT → BTCUSDT)"""
    return raw_symbol.replace("/USDT:USDT", "USDT").replace("/", "")


async def _post_to_central(path: str, payload: dict):
    """공통 메인서버 콜백"""
    if not settings.central_url:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            resp = await http.post(
                f"{settings.central_url}{path}",
                json=payload,
                headers={"Authorization": f"Bearer {settings.agent_token}"},
            )
            if resp.status_code == 200:
                logger.info(f"[ManualDetect] {path} 콜백 성공")
            else:
                logger.warning(f"[ManualDetect] {path} 콜백 실패: {resp.status_code} {resp.text}")
    except Exception as e:
        logger.error(f"[ManualDetect] {path} 콜백 오류: {e}")


async def _notify_manual_position(symbol: str, pos: dict, is_addon: bool):
    """신규진입 or 추가매수 콜백"""
    payload = {
        "symbol": symbol,
        "side": pos.get("side", ""),
        "qty": str(pos.get("contracts", "0")),
        "entry_price": str(pos.get("entryPrice", "0")),
        "leverage": int(pos.get("leverage", 10)),
        "is_addon": is_addon,
        "mark_price": str(pos.get("markPrice") or "0"),
    }
    await _post_to_central("/api/agent/manual-position", payload)


async def _notify_position_closed(symbol: str):
    """청산 콜백 — Bybit에서 closed PnL 조회 후 전송"""
    import time
    client = get_exchange_client()
    payload: dict = {"symbol": symbol}
    pnl_info = None
    detected_at_ms = int(time.time() * 1000)  # 포지션 청산 감지 시각

    for attempt in range(6):
        try:
            candidate = await client.get_closed_pnl(symbol)
            if candidate:
                created_time = int(candidate.get("createdTime", 0))
                # 감지 시각 기준 120초 이내 데이터만 유효로 판단 (stale 데이터 제거)
                if created_time > detected_at_ms - 120_000:
                    pnl_info = candidate
                    break
                else:
                    logger.warning(
                        f"[ManualDetect] stale pnl data skipped: createdTime={created_time}, "
                        f"detected_at={detected_at_ms}"
                    )
        except Exception as e:
            logger.warning(f"[ManualDetect] closed_pnl 조회 실패 (attempt {attempt + 1}): {e}")
        if attempt < 5:
            await asyncio.sleep(5)
    if pnl_info:
        if pnl_info.get("avgExitPrice"):
            payload["exit_price"] = str(pnl_info["avgExitPrice"])
        if pnl_info.get("closedPnl") is not None:
            payload["realized_pnl"] = str(pnl_info["closedPnl"])
    else:
        # closed_pnl 조회 실패 시 fills로 exit_price 추정 (Bybit API 지연 대응)
        try:
            fills = await client.get_recent_fills(symbol, limit=5)
            recent = [f for f in fills if int(f.get("execTime", 0)) > detected_at_ms - 120_000]
            if recent:
                total_qty = sum(float(f.get("execQty", 0)) for f in recent)
                if total_qty > 0:
                    avg_price = sum(float(f["execPrice"]) * float(f["execQty"]) for f in recent) / total_qty
                    payload["exit_price"] = str(round(avg_price, 8))
                    logger.info(f"[ManualDetect] fills fallback exit_price={avg_price:.8f} ({len(recent)} fills)")
                total_fee = sum(float(f.get("execFee", 0)) for f in recent)
                if total_fee > 0:
                    payload["commission"] = str(round(total_fee, 8))
        except Exception as e:
            logger.warning(f"[ManualDetect] fills fallback failed: {e}")
    await _post_to_central("/api/agent/position-closed", payload)


async def _notify_position_heartbeat(symbol: str, pos: dict):
    """MAE/MFE 업데이트용 heartbeat"""
    payload = {
        "symbol": symbol,
        "mark_price": str(pos.get("markPrice") or "0"),
        "unrealized_pnl": str(pos.get("unrealizedPnl") or "0"),
    }
    await _post_to_central("/api/agent/position-heartbeat", payload)


async def _notify_tp_filled(symbol: str, pos: dict, prev_qty: float, curr_qty: float):
    """TP 부분 체결 콜백"""
    qty_closed = round(prev_qty - curr_qty, 9)
    client = get_exchange_client()
    actual_exit_price = None
    best_fill = None
    try:
        fills = await client.get_recent_fills(symbol, limit=5)
        if fills:
            best_fill = min(fills, key=lambda f: abs(float(f.get("execQty", 0)) - qty_closed))
            actual_exit_price = best_fill.get("execPrice")
    except Exception as e:
        logger.warning(f"[TpFilled] fills 조회 실패: {e}")

    payload = {
        "symbol": symbol,
        "mark_price": str(pos.get("markPrice") or "0"),
        "exit_price": str(actual_exit_price) if actual_exit_price else None,
        "qty_closed": str(qty_closed),
        "remaining_qty": str(curr_qty),
        "commission": str(best_fill.get("execFee", 0)) if best_fill else None,
    }
    await _post_to_central("/api/agent/tp-filled", payload)


async def detect_manual_positions():
    """30초마다 포지션 폴링 — 수동 진입/추가매수/청산 감지 + MAE/MFE heartbeat"""
    global _known_positions, _bot_executed_symbols
    await asyncio.sleep(15)  # 시작 직후 1회 초기화
    client = get_exchange_client()

    # 초기 포지션 스냅샷 (서버 시작 시 이미 있던 포지션은 수동 감지 스킵)
    try:
        existing = await client.get_all_positions()
        for pos in existing:
            raw_symbol = pos.get("symbol", "")
            symbol = _normalize_symbol(raw_symbol)
            _known_positions[symbol] = pos
        logger.info(f"Initial position snapshot: {list(_known_positions.keys())}")
    except Exception as e:
        logger.error(f"Initial position snapshot failed: {e}")

    while True:
        await asyncio.sleep(30)
        try:
            current_positions = await client.get_all_positions()
            current_map = {_normalize_symbol(p.get("symbol", "")): p for p in current_positions}

            for symbol, pos in current_map.items():
                known = _known_positions.get(symbol)

                if known is None:
                    # 신규 포지션
                    if symbol in _bot_executed_symbols:
                        _bot_executed_symbols.discard(symbol)
                        logger.info(f"[ManualDetect] 봇 진입 감지 스킵: {symbol}")
                    else:
                        logger.info(f"[ManualDetect] 수동 포지션 감지: {symbol}")
                        await _notify_manual_position(symbol, pos, is_addon=False)
                else:
                    # qty 증가 → 수동 추가매수
                    prev_qty = float(known.get("contracts", 0) or 0)
                    curr_qty = float(pos.get("contracts", 0) or 0)
                    if curr_qty > prev_qty + 1e-9:
                        if symbol in _bot_executed_symbols:
                            _bot_executed_symbols.discard(symbol)
                            logger.info(f"[ManualDetect] 봇 추가매수 감지 스킵: {symbol}")
                        else:
                            logger.info(
                                f"[ManualDetect] 수동 추가매수 감지: {symbol} "
                                f"qty {prev_qty} → {curr_qty}"
                            )
                            await _notify_manual_position(symbol, pos, is_addon=True)
                    elif curr_qty < prev_qty - 1e-9:
                        # qty 감소 + 포지션 존재 → TP 부분 체결
                        logger.info(
                            f"[ManualDetect] TP 부분 체결 감지: {symbol} "
                            f"qty {prev_qty} → {curr_qty}"
                        )
                        await _notify_tp_filled(symbol, pos, prev_qty, curr_qty)

                    # MAE/MFE heartbeat (보유 중 포지션마다 30초마다 전송)
                    await _notify_position_heartbeat(symbol, pos)

                _known_positions[symbol] = pos

            # 청산 감지
            for symbol in list(_known_positions.keys()):
                if symbol not in current_map:
                    known = _known_positions[symbol]
                    prev_qty = float(known.get("contracts", 0) or 0)

                    # SL vs TP 구분: recent fills의 orderType으로 판단
                    if prev_qty > 1e-9:
                        is_tp_fill = False
                        try:
                            fills = await client.get_recent_fills(symbol, limit=3)
                            if fills:
                                latest = fills[0]
                                order_type = latest.get("orderType", "")
                                stop_order_type = latest.get("stopOrderType", "")
                                # TP limit order: orderType="Limit", stopOrderType 없음
                                # SL market order: orderType="Market" 또는 stopOrderType 있음
                                is_tp_fill = (order_type == "Limit" and not stop_order_type)
                        except Exception as e:
                            logger.warning(f"[ManualDetect] fills 조회 실패, tp_filled 스킵: {e}")

                        if is_tp_fill:
                            logger.info(
                                f"[ManualDetect] 포지션 소멸 (마지막 TP 체결): {symbol} "
                                f"qty {prev_qty} → 0"
                            )
                            await _notify_tp_filled(symbol, known, prev_qty, 0.0)
                        else:
                            logger.info(
                                f"[ManualDetect] 포지션 소멸 (SL/수동 청산): {symbol} "
                                f"qty {prev_qty} → 0"
                            )

                    logger.info(f"[ManualDetect] 포지션 청산 감지: {symbol}")
                    await _notify_position_closed(symbol)
                    del _known_positions[symbol]

        except Exception as e:
            logger.error(f"[ManualDetect] 폴링 오류: {e}")


# ===== Lifespan =====

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Railway는 RAILWAY_PUBLIC_DOMAIN을 자동 제공
    railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if railway_domain:
        agent_url = f"https://{railway_domain}"
        logger.info(f"Agent URL: {agent_url}")
        await register_with_central(agent_url)
    else:
        logger.warning("RAILWAY_PUBLIC_DOMAIN not set - skipping registration (local mode)")

    asyncio.create_task(detect_manual_positions())
    logger.info(f"Agent started: exchange={settings.exchange}, user={settings.user_id[:8]}...")
    yield
    logger.info("Agent shutting down")


# ===== FastAPI 앱 =====

app = FastAPI(
    title="AutoTrading User Agent",
    version="1.0.0",
    description="User-side trading execution agent",
    lifespan=lifespan
)


# ===== 주문 실행 엔드포인트 =====

@app.post("/execute")
async def execute_order(request: ExecuteRequest):
    """
    중앙 서버에서 전달받은 주문 실행

    보안:
    - HMAC-SHA256 서명 검증
    - Timestamp 60초 초과 거부
    - expected_position_size 검증 (포지션 불일치 감지)
    """
    # 요청을 dict로 변환 (hmac_signature 검증용)
    # exclude_unset=True: JSON에 없던 필드(Pydantic 기본값 None)를 제외해 메인서버 canonical과 일치시킴
    payload = request.model_dump(exclude_unset=True)

    # 1. Timestamp 검증
    if not check_timestamp(payload.get("timestamp", "")):
        raise HTTPException(status_code=400, detail="Timestamp expired or invalid (max 60s)")

    # 2. HMAC 서명 검증 (payload를 복사해서 검증 — pop하므로 원본 보존)
    payload_copy = dict(payload)
    if not verify_hmac_signature(payload_copy):
        logger.warning(f"HMAC verification failed for order_type={request.order_type}")
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

    client = get_exchange_client()
    symbol = request.symbol

    try:
        # ===== 주문 타입별 처리 =====

        if request.order_type == "set_leverage":
            if not request.leverage:
                raise HTTPException(status_code=400, detail="leverage required")
            success = await client.set_leverage(symbol, request.leverage)
            return {"success": success}

        elif request.order_type == "market_entry":
            # 3. expected_position_size 검증 (진입 전 포지션 없어야 함)
            if request.expected_position_size is not None:
                position = await client.get_position(symbol)
                actual = float(position.qty) if position else 0.0
                if abs(actual - request.expected_position_size) > 0.0001:
                    logger.warning(
                        f"Position mismatch: expected={request.expected_position_size}, actual={actual}"
                    )
                    return {
                        "success": False,
                        "reason": "position_mismatch",
                        "expected": request.expected_position_size,
                        "actual": actual,
                    }

            if not request.qty or not request.side:
                raise HTTPException(status_code=400, detail="qty and side required for market_entry")

            # 레버리지 설정
            if request.leverage:
                await client.switch_to_one_way_mode(symbol)
                await client.set_leverage(symbol, request.leverage)

            # 시장가 진입
            order_id = await client.place_market_order(
                symbol, request.side, Decimal(request.qty)
            )

            # SL 설정
            if request.sl_price:
                await client.set_stop_loss(symbol, Decimal(str(request.sl_price)))

            # TP 주문들
            tp_order_ids = []
            if request.tp_orders:
                for tp in request.tp_orders:
                    tp_id = await client.place_tp_order(
                        symbol,
                        tp["side"],
                        Decimal(str(tp["qty"])),
                        Decimal(str(tp["price"]))
                    )
                    if tp_id:
                        tp_order_ids.append(tp_id)

            # DCA 추가매수 지정가 주문들
            dca_order_ids = []
            if request.dca_orders:
                for dca in request.dca_orders:
                    dca_id = await client.place_limit_order(
                        symbol,
                        dca["side"],
                        Decimal(str(dca["qty"])),
                        Decimal(str(dca["price"]))
                    )
                    if dca_id:
                        dca_order_ids.append(dca_id)

            _bot_executed_symbols.add(symbol)
            logger.info(
                f"market_entry completed: {symbol} {request.side} {request.qty} "
                f"(market={order_id}, tp={len(tp_order_ids)}, dca={len(dca_order_ids)})"
            )
            return {
                "success": True,
                "order_id": order_id,
                "tp_order_ids": tp_order_ids,
                "dca_order_ids": dca_order_ids,
            }

        elif request.order_type == "close":
            # expected_position_size 검증
            if request.expected_position_size is not None:
                position = await client.get_position(symbol)
                actual = float(position.qty) if position else 0.0
                if abs(actual - request.expected_position_size) > 0.0001:
                    logger.warning(
                        f"Position mismatch on close: expected={request.expected_position_size}, actual={actual}"
                    )
                    return {
                        "success": False,
                        "reason": "position_mismatch",
                        "expected": request.expected_position_size,
                        "actual": actual,
                    }

            # 미체결 주문 취소
            await client.cancel_all_orders(symbol)

            # 포지션 청산
            position = await client.get_position(symbol)
            if position:
                success = await client.close_position(symbol, position.side)
                logger.info(f"close completed: {symbol} {position.side}")
                return {"success": success}
            else:
                logger.warning(f"No position to close for {symbol}")
                return {"success": True, "message": "No position found"}

        elif request.order_type == "set_sl":
            if request.sl_price is None:
                raise HTTPException(status_code=400, detail="sl_price required")
            success = await client.set_stop_loss(symbol, Decimal(str(request.sl_price)))
            return {"success": success}

        elif request.order_type == "cancel_order":
            if not request.order_id:
                raise HTTPException(status_code=400, detail="order_id required")
            success = await client.cancel_order(symbol, request.order_id)
            return {"success": success}

        elif request.order_type == "cancel_all":
            success = await client.cancel_all_orders(symbol)
            return {"success": success}

        elif request.order_type == "adjust":
            # 재조정: 기존 주문 전체 취소 → SL 설정 → TP/DCA 주문 재배치
            await client.cancel_all_orders(symbol)

            # 최신 포지션 조회 (DCA 체결로 인한 수량 변동 반영)
            position = await client.get_position(symbol)
            position_qty = position.qty if position else Decimal("0")

            sl_set = False
            if request.sl_price:
                sl_set = await client.set_stop_loss(symbol, Decimal(str(request.sl_price)))

            tp_order_ids = []
            if request.tp_orders:
                for tp in request.tp_orders:
                    tp_id = await client.place_tp_order(
                        symbol,
                        tp["side"],
                        Decimal(str(tp["qty"])),
                        Decimal(str(tp["price"]))
                    )
                    if tp_id:
                        tp_order_ids.append(tp_id)

            dca_order_ids = []
            if request.dca_orders:
                for dca in request.dca_orders:
                    dca_id = await client.place_limit_order(
                        symbol,
                        dca["side"],
                        Decimal(str(dca["qty"])),
                        Decimal(str(dca["price"]))
                    )
                    if dca_id:
                        dca_order_ids.append(dca_id)

            logger.info(
                f"adjust completed: {symbol} sl={request.sl_price} "
                f"(tp={len(tp_order_ids)}, dca={len(dca_order_ids)}, position_qty={position_qty})"
            )
            return {
                "success": True,
                "position_qty": str(position_qty),
                "sl_set": sl_set,
                "tp_order_ids": tp_order_ids,
                "dca_order_ids": dca_order_ids,
            }

        else:
            raise HTTPException(status_code=400, detail=f"Unknown order_type: {request.order_type}")

    except HTTPException:
        raise
    except ExchangeError as e:
        logger.error(f"Exchange error in execute [{request.order_type}]: {e}")
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in execute [{request.order_type}]: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ===== 공개 핑 (Railway 헬스체크용 — 인증 불필요) =====

@app.get("/ping")
async def ping():
    return {"status": "ok"}


# ===== 헬스체크 =====

@app.get("/health")
async def health_check(authorization: str = Header(None)):
    """헬스체크 (exchange 연결 + 잔고 확인)"""
    if not verify_bearer_token(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    client = get_exchange_client()
    try:
        balance = await client.get_balance()
        return {
            "status": "healthy",
            "exchange": settings.exchange,
            "user_id": settings.user_id,
            "balance_usdt": float(balance) if balance is not None else None,
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )


# ===== 포지션 조회 =====

@app.get("/position")
async def get_position(symbol: str, authorization: str = Header(None)):
    """포지션 조회"""
    if not verify_bearer_token(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    client = get_exchange_client()
    try:
        position = await client.get_position(symbol)
        if position is None:
            return {"has_position": False, "symbol": symbol}
        return {
            "has_position": True,
            "symbol": position.symbol,
            "side": position.side,
            "qty": str(position.qty),
            "entry_price": str(position.entry_price),
            "leverage": position.leverage,
            "unrealized_pnl": str(position.unrealized_pnl),
            "stop_loss": str(position.stop_loss) if position.stop_loss else None,
            "mark_price": str(position.mark_price) if position.mark_price else None,
        }
    except ExchangeError as e:
        raise HTTPException(status_code=502, detail=str(e))


# ===== 잔고 조회 =====

@app.get("/balance")
async def get_balance(authorization: str = Header(None)):
    """USDT 가용 잔고 조회"""
    if not verify_bearer_token(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    client = get_exchange_client()
    try:
        balance = await client.get_balance()
        return {
            "usdt_balance": float(balance) if balance is not None else None,
            "exchange": settings.exchange,
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ===== 거래소 UID 조회 =====

@app.get("/uid")
async def get_uid(authorization: str = Header(None)):
    """거래소 계정 UID 조회 (레퍼럴 검증용)"""
    if not verify_bearer_token(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    client = get_exchange_client()
    try:
        uid = await client.get_account_uid()
        return {
            "uid": uid,
            "exchange": settings.exchange,
            "referral_code": settings.referral_code,
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ===== 현재가 조회 =====

@app.get("/price")
async def get_price(symbol: str, authorization: str = Header(None)):
    """현재가 조회"""
    if not verify_bearer_token(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    client = get_exchange_client()
    try:
        price = await client.get_current_price(symbol)
        return {
            "symbol": symbol,
            "price": float(price) if price is not None else None,
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ===== 청산 PnL 조회 =====

@app.get("/closed-pnl")
async def get_closed_pnl_endpoint(symbol: str, authorization: str = Header(None)):
    """청산 PnL 조회 (중앙서버 close 후 호출 — 최대 4회 재시도)"""
    if not verify_bearer_token(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")

    client = get_exchange_client()
    for attempt in range(4):
        try:
            pnl_info = await client.get_closed_pnl(symbol)
            if pnl_info:
                return {
                    "found": True,
                    "symbol": symbol,
                    "exit_price": str(pnl_info["avgExitPrice"]) if pnl_info.get("avgExitPrice") else None,
                    "realized_pnl": str(pnl_info["closedPnl"]) if pnl_info.get("closedPnl") is not None else None,
                }
        except Exception as e:
            logger.warning(f"get_closed_pnl attempt {attempt + 1} failed: {e}")
        if attempt < 3:
            await asyncio.sleep(3)

    return {"found": False, "symbol": symbol, "exit_price": None, "realized_pnl": None}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.port)
