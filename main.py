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
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Literal

import httpx
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel, Field

from config import settings
from exchange_client import AgentExchangeClient, ExchangeError

limiter = Limiter(key_func=get_remote_address)

# ===== Replay attack 방지 nonce 캐시 =====
_used_nonces: dict[str, float] = {}

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
_pending_bot_dca_ids: Dict[str, set] = {}          # 봇이 등록한 DCA 지정가 주문 ID (심볼 → set of order_id)
_trailing_stop_active: set = set()                  # trailing stop이 활성화된 심볼 집합
_known_sl_prices: Dict[str, str] = {}              # symbol → SL 가격 문자열 (MANUAL_CLOSE 오분류 방지)


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
    order_type: Literal["market_entry", "close", "set_sl", "set_leverage", "cancel_order", "cancel_all", "adjust"]
    symbol: str = Field(pattern=r"^[A-Z0-9]{2,20}$")
    side: Optional[str] = None          # "Buy" or "Sell"
    qty: Optional[str] = None           # 수량 문자열 (정밀도 처리는 거래소 클라이언트에서)
    price: Optional[str] = None         # 지정가 주문 가격 문자열
    sl_price: Optional[float] = None
    trailing_stop_distance: Optional[float] = None   # TP1 체결 후 트레일링 스탑 거리 (USDT, 1x ATR)
    tp_orders: Optional[List[dict]] = None   # [{"side": "Sell", "qty": "0.001", "price": "45000.0"}]
    dca_orders: Optional[List[dict]] = None  # DCA 추가매수 지정가 주문 목록 (market_entry와 함께)
    leverage: Optional[int] = None
    order_id: Optional[str] = None
    expected_position_size: Optional[float] = None  # None = 검증 생략
    timestamp: str              # ISO 형식


# ===== 중앙 서버 등록 =====

async def register_with_central(agent_url: str):
    """중앙 서버에 Agent URL 등록"""
    await asyncio.sleep(3)  # 서버가 요청을 받을 수 있을 때까지 대기
    try:
        payload = {
            "agent_url": agent_url,
            "exchange": settings.exchange,
            "partner_code": settings.partner_code,
        }
        payload_bytes = json.dumps(payload, sort_keys=True).encode()
        sig = hmac.new(
            settings.token_secret.encode(),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        async with httpx.AsyncClient(verify=True, timeout=10.0) as client:
            resp = await client.post(
                f"{settings.central_url_normalized}/api/agent/register",
                content=payload_bytes,
                headers={
                    "Authorization": f"Bearer {settings.agent_token}",
                    "X-Agent-Signature": sig,
                    "Content-Type": "application/json",
                },
            )
            if resp.status_code == 200:
                logger.info("Successfully registered with central server")
            else:
                logger.warning(f"Registration failed: HTTP {resp.status_code}")
    except Exception as e:
        logger.error("Failed to register with central server")


# ===== 수동 포지션 감지 =====

def _normalize_symbol(raw_symbol: str) -> str:
    """ccxt 심볼 형식 → Bybit 형식 변환 (BTC/USDT:USDT → BTCUSDT)"""
    return raw_symbol.replace("/USDT:USDT", "USDT").replace("/", "")


async def _post_to_central(path: str, payload: dict):
    """공통 메인서버 콜백 (HMAC-SHA256 서명 포함)"""
    if not settings.central_url:
        return
    try:
        payload_bytes = json.dumps(payload, sort_keys=True, default=str).encode()
        sig = hmac.new(
            settings.token_secret.encode(),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        async with httpx.AsyncClient(verify=True, timeout=30.0) as http:
            resp = await http.post(
                f"{settings.central_url_normalized}{path}",
                content=payload_bytes,
                headers={
                    "Authorization": f"Bearer {settings.agent_token}",
                    "X-Agent-Signature": sig,
                    "Content-Type": "application/json",
                },
            )
            if resp.status_code == 200:
                logger.info("[ManualDetect] 콜백 성공")
            else:
                logger.warning(f"[ManualDetect] 콜백 실패: HTTP {resp.status_code}")
    except Exception as e:
        logger.error("[ManualDetect] 콜백 오류 발생")


_NETWORK_ERROR_KEYWORDS = ("timeout", "connection", "network", "502", "503", "504")


async def _post_error_to_central(error_type: str, file_name: str, function_name: str, message: str):
    """오류를 메인서버에 리포팅 (네트워크 오류 제외, fire-and-forget)"""
    if not settings.central_url:
        return
    msg_lower = message.lower()
    if any(kw in msg_lower for kw in _NETWORK_ERROR_KEYWORDS):
        return
    try:
        payload = {
            "error_type": error_type,
            "file_name": file_name,
            "function_name": function_name,
            "message": message[:500],
        }
        payload_bytes = json.dumps(payload, sort_keys=True, default=str).encode()
        sig = hmac.new(
            settings.token_secret.encode(),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        async with httpx.AsyncClient(verify=True, timeout=10.0) as http:
            await http.post(
                f"{settings.central_url_normalized}/api/agent/error",
                content=payload_bytes,
                headers={
                    "Authorization": f"Bearer {settings.agent_token}",
                    "X-Agent-Signature": sig,
                    "Content-Type": "application/json",
                },
            )
    except Exception:
        logger.warning("[AgentError] 오류 리포팅 실패 (무시)")


async def _notify_manual_position(symbol: str, pos: dict, is_addon: bool, is_bot_dca: bool = False):
    """신규진입 or 추가매수 콜백. is_bot_dca=True이면 서버에서 Telegram 생략, AI 재분석만 실행."""
    payload = {
        "symbol": symbol,
        "side": pos.get("side", ""),
        "qty": str(pos.get("contracts", "0")),
        "entry_price": str(pos.get("entryPrice", "0")),
        "leverage": int(pos.get("leverage", 10)),
        "is_addon": is_addon,
        "is_bot_dca": is_bot_dca,
        "mark_price": str(pos.get("markPrice") or "0"),
    }
    await _post_to_central("/api/agent/manual-position", payload)


async def _notify_position_closed(symbol: str, exit_reason_override: Optional[str] = None):
    """청산 콜백 — closed PnL 조회 후 전송. exit_reason_override가 있으면 우선 적용."""
    import time
    client = get_exchange_client()
    payload: dict = {"symbol": symbol}
    if exit_reason_override:
        payload["exit_reason"] = exit_reason_override
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
        # stopOrderType / orderType으로 exit_reason 판정
        # SL: stopOrderType="StopLoss" / 수동: Market + stopOrderType 없음 / TP: Limit → 미설정(서버 DB 판단)
        try:
            fills = await client.get_recent_fills(symbol, limit=5)
            recent_fills = [f for f in fills if int(f.get("execTime", 0)) > detected_at_ms - 120_000]
            if recent_fills:
                any_sl = any(f.get("stopOrderType") == "StopLoss" for f in recent_fills)
                any_tp_limit = any(
                    f.get("orderType") == "Limit" and not f.get("stopOrderType")
                    for f in recent_fills
                )
                # Bitget plan order SL 체결은 stopOrderType이 없을 수 있음
                # → exit_price와 known SL 가격 비교로 보완 판정
                if not any_sl and symbol in _known_sl_prices:
                    known_sl = float(_known_sl_prices[symbol])
                    exit_price_val = float(payload.get("exit_price", 0) or 0)
                    if exit_price_val > 0 and abs(exit_price_val - known_sl) / known_sl < 0.005:
                        any_sl = True
                        logger.info(
                            f"[ManualDetect] SL_HIT 판정 (price match): "
                            f"exit={exit_price_val} ≈ sl={known_sl} ({symbol})"
                        )
                # exit_reason_override가 있으면 fills 판정 결과로 덮어쓰지 않음
                if "exit_reason" not in payload:
                    if any_sl:
                        payload["exit_reason"] = "SL_HIT"
                    elif not any_tp_limit:
                        payload["exit_reason"] = "MANUAL_CLOSE"
                    # TP Limit 체결: exit_reason 미포함 → 서버 tp_filled DB 판단
                logger.info(
                    f"[ManualDetect] exit_reason={payload.get('exit_reason', 'none(TP)')} (stopOrderType check)"
                )
        except Exception as e:
            logger.warning(f"[ManualDetect] stopOrderType 조회 실패, 서버 fallback 사용: {e}")
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
                # exit_reason_override가 있으면 fills 판정 결과로 덮어쓰지 않음
                if "exit_reason" not in payload:
                    any_sl = any(f.get("stopOrderType") == "StopLoss" for f in recent)
                    any_tp_limit = any(
                        f.get("orderType") == "Limit" and not f.get("stopOrderType")
                        for f in recent
                    )
                    # Bitget plan order SL 체결 보완 판정 (fills fallback 경로)
                    if not any_sl and symbol in _known_sl_prices:
                        known_sl = float(_known_sl_prices[symbol])
                        exit_price_val = float(payload.get("exit_price", 0) or 0)
                        if exit_price_val > 0 and abs(exit_price_val - known_sl) / known_sl < 0.005:
                            any_sl = True
                            logger.info(
                                f"[ManualDetect] SL_HIT 판정 (price match, fallback): "
                                f"exit={exit_price_val} ≈ sl={known_sl} ({symbol})"
                            )
                    if any_sl:
                        payload["exit_reason"] = "SL_HIT"
                    elif not any_tp_limit:
                        payload["exit_reason"] = "MANUAL_CLOSE"
                logger.info(
                    f"[ManualDetect] exit_reason={payload.get('exit_reason', 'none(TP)')} (fills fallback stopOrderType check)"
                )
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


async def _notify_direction_switch(symbol: str, old_pos: dict, new_pos: dict):
    """방향 전환 콜백 (LONG→SHORT or SHORT→LONG)"""
    import time
    client = get_exchange_client()
    detected_at_ms = int(time.time() * 1000)

    new_side_raw = (new_pos.get("side") or "").lower()
    new_direction = "SHORT" if new_side_raw == "short" else "LONG"

    payload = {
        "symbol": symbol,
        "new_direction": new_direction,
        "new_qty": str(new_pos.get("contracts", "0")),
        "new_entry_price": str(new_pos.get("entryPrice", "0")),
    }

    # 기존 포지션 청산 PnL 조회
    for attempt in range(6):
        try:
            candidate = await client.get_closed_pnl(symbol)
            if candidate:
                created_time = int(candidate.get("createdTime", 0))
                if created_time > detected_at_ms - 120_000:
                    if candidate.get("avgExitPrice"):
                        payload["exit_price"] = str(candidate["avgExitPrice"])
                    if candidate.get("closedPnl") is not None:
                        payload["realized_pnl"] = str(candidate["closedPnl"])
                    break
        except Exception as e:
            logger.warning(f"[ManualDetect] direction_switch pnl 조회 실패 (attempt {attempt + 1}): {e}")
        if attempt < 5:
            await asyncio.sleep(5)

    await _post_to_central("/api/agent/direction-switch", payload)


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
    except Exception:
        logger.warning("[TpFilled] fills 조회 실패")

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
    global _known_positions, _bot_executed_symbols, _trailing_stop_active, _known_sl_prices
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
                    # 방향 전환 감지 (LONG↔SHORT)
                    prev_side = (known.get("side") or "").lower()
                    curr_side = (pos.get("side") or "").lower()
                    if prev_side and curr_side and prev_side != curr_side:
                        logger.info(f"[ManualDetect] 방향 전환 감지: {symbol}")
                        await _notify_direction_switch(symbol, known, pos)
                        _known_positions[symbol] = pos
                        continue

                    # qty 증가 → 수동 추가매수
                    prev_qty = float(known.get("contracts", 0) or 0)
                    curr_qty = float(pos.get("contracts", 0) or 0)
                    if curr_qty > prev_qty + 1e-9:
                        if symbol in _bot_executed_symbols:
                            _bot_executed_symbols.discard(symbol)
                            logger.info(f"[ManualDetect] 봇 추가매수 감지 스킵: {symbol}")
                        elif _pending_bot_dca_ids.get(symbol):
                            _pending_bot_dca_ids[symbol].pop()
                            if not _pending_bot_dca_ids[symbol]:
                                del _pending_bot_dca_ids[symbol]
                            logger.info(f"[ManualDetect] 봇 DCA 체결 감지: {symbol} — AI 재분석 트리거")
                            await _notify_manual_position(symbol, pos, is_addon=True, is_bot_dca=True)
                        else:
                            logger.info(f"[ManualDetect] 수동 추가매수 감지: {symbol}")
                            await _notify_manual_position(symbol, pos, is_addon=True)
                    elif curr_qty < prev_qty - 1e-9:
                        # qty 감소 + 포지션 존재 → TP 부분 체결
                        logger.info(f"[ManualDetect] TP 부분 체결 감지: {symbol}")
                        await _notify_tp_filled(symbol, pos, prev_qty, curr_qty)

                    # MAE/MFE heartbeat (보유 중 포지션마다 30초마다 전송)
                    await _notify_position_heartbeat(symbol, pos)

                _known_positions[symbol] = pos

            # 청산 감지
            for symbol in list(_known_positions.keys()):
                if symbol not in current_map:
                    known = _known_positions[symbol]
                    prev_qty = float(known.get("contracts", 0) or 0)
                    had_trailing_stop = symbol in _trailing_stop_active  # 청산 전 상태 캡처

                    # cancel_all_orders 이전에 trailing stop pending 여부 확인
                    # 발동했으면 Bitget open orders에 없음 → trailing_stop_fired=True
                    # 수동청산/SL이면 아직 pending → trailing_stop_fired=False
                    trailing_stop_fired = False
                    if had_trailing_stop:
                        trailing_stop_pending = await client.has_pending_trailing_stop(symbol)
                        trailing_stop_fired = not trailing_stop_pending
                        logger.info(
                            f"[ManualDetect] trailing stop 상태: "
                            f"pending={trailing_stop_pending}, fired={trailing_stop_fired} ({symbol})"
                        )

                    # Pending 상태 plan order(trailing stop 포함) 명시적 취소
                    # Bitget은 포지션 청산 시 plan order를 자동으로 취소하지 않음
                    try:
                        await client.cancel_all_orders(symbol)
                    except Exception as e:
                        logger.warning(f"[ManualDetect] 청산 후 주문 취소 실패: {symbol}: {e}")
                    _trailing_stop_active.discard(symbol)

                    # SL vs TP vs TRAILING_STOP 구분
                    detected_exit_reason = None
                    if prev_qty > 1e-9:
                        is_tp_fill = False
                        try:
                            fills = await client.get_recent_fills(symbol, limit=3)
                            if fills:
                                latest = fills[0]
                                order_type = latest.get("orderType", "")
                                stop_order_type = latest.get("stopOrderType", "")
                                # TP limit order: orderType="Limit", stopOrderType 없음
                                # SL market order: orderType="Market" + stopOrderType="StopLoss"
                                # Trailing stop: trailing_stop_fired=True (cancel 전 open orders 조회로 확정)
                                is_tp_fill = (order_type == "Limit" and not stop_order_type)
                                if trailing_stop_fired and not is_tp_fill:
                                    detected_exit_reason = "TRAILING_STOP"
                                    logger.info(f"[ManualDetect] 트레일링 스탑 발동 감지: {symbol}")
                        except Exception:
                            logger.warning("[ManualDetect] fills 조회 실패, tp_filled 스킵")

                        if is_tp_fill:
                            logger.info(f"[ManualDetect] 포지션 소멸 (마지막 TP 체결): {symbol}")
                            await _notify_tp_filled(symbol, known, prev_qty, 0.0)
                        elif detected_exit_reason == "TRAILING_STOP":
                            logger.info(f"[ManualDetect] 포지션 소멸 (트레일링 스탑 발동): {symbol}")
                        else:
                            logger.info(f"[ManualDetect] 포지션 소멸 (SL/수동 청산): {symbol}")

                    logger.info(f"[ManualDetect] 포지션 청산 감지: {symbol}")
                    await _notify_position_closed(symbol, exit_reason_override=detected_exit_reason)
                    del _known_positions[symbol]
                    _known_sl_prices.pop(symbol, None)
                    _pending_bot_dca_ids.pop(symbol, None)

        except Exception as e:
            logger.error(f"[ManualDetect] 폴링 오류: {e}")
            await _post_error_to_central("logic_error", "main.py", "detect_manual_positions", str(e))


async def _run_polling_supervisor():
    """폴링 루프 슈퍼바이저 — 예외로 종료 시 30초 후 재시작"""
    while True:
        try:
            await detect_manual_positions()
        except Exception as e:
            logger.error(f"[ManualDetect] 루프 비정상 종료, 30초 후 재시작: {e}")
            await _post_error_to_central("logic_error", "main.py", "_run_polling_supervisor", str(e))
            await asyncio.sleep(30)


# ===== Lifespan =====

@asynccontextmanager
async def lifespan(app: FastAPI):
    # CENTRAL_URL HTTPS 강제 검증
    if settings.central_url and not settings.central_url.startswith("https://"):
        raise RuntimeError(f"CENTRAL_URL must use HTTPS (got: {settings.central_url[:10]}...)")

    # Railway는 RAILWAY_PUBLIC_DOMAIN을 자동 제공
    railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if railway_domain:
        agent_url = f"https://{railway_domain}"
        logger.info(f"Agent URL: {agent_url}")
        asyncio.create_task(register_with_central(agent_url))
    else:
        logger.warning(
            "RAILWAY_PUBLIC_DOMAIN not set - 앱과 연동이 되지 않습니다. "
            "Railway 서비스에서 Settings → Networking → Generate Domain으로 퍼블릭 도메인을 생성하세요."
        )

    asyncio.create_task(_run_polling_supervisor())
    logger.info(f"Agent started: exchange={settings.exchange}")
    yield
    logger.info("Agent shutting down")


# ===== FastAPI 앱 =====

app = FastAPI(
    title="AutoTrading User Agent",
    version="1.0.0",
    description="User-side trading execution agent",
    lifespan=lifespan
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# ===== 주문 실행 엔드포인트 =====

@app.post("/execute")
@limiter.limit("30/minute")
async def execute_order(request: Request, execute_req: ExecuteRequest):
    request_obj = request
    raw_body = await request_obj.body()
    request = execute_req
    """
    중앙 서버에서 전달받은 주문 실행

    보안:
    - HMAC-SHA256 서명 검증 (raw body bytes vs X-Hmac-Signature 헤더)
    - Timestamp 60초 초과 거부
    - expected_position_size 검증 (포지션 불일치 감지)
    """
    # 1. Timestamp 검증
    if not check_timestamp(request.timestamp):
        raise HTTPException(status_code=400, detail="Timestamp expired or invalid (max 60s)")

    # 2. HMAC 서명 검증: raw body bytes 기반 (Pydantic 재직렬화 없음)
    x_hmac_sig = request_obj.headers.get("x-hmac-signature", "")
    expected = hmac.new(
        settings.token_secret.encode(),
        raw_body,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, x_hmac_sig):
        logger.warning(f"HMAC verification failed for order_type={request.order_type}")
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

    # 3. Nonce 중복 방지 (60초 창 내 동일 서명 재전송 차단)
    sig_val = x_hmac_sig
    _now = time.time()
    expired_keys = [k for k, v in _used_nonces.items() if _now - v > 60]
    for k in expired_keys:
        del _used_nonces[k]
    if sig_val in _used_nonces:
        raise HTTPException(status_code=400, detail="Duplicate request")
    _used_nonces[sig_val] = _now

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

            # 시장가 진입 (플래그를 주문 전에 설정해야 polling loop와의 race condition 방지)
            _bot_executed_symbols.add(symbol)
            order_id = await client.place_market_order(
                symbol, request.side, Decimal(request.qty)
            )

            # SL 설정
            if request.sl_price:
                await client.set_stop_loss(symbol, Decimal(str(request.sl_price)))
                _known_sl_prices[symbol] = str(request.sl_price)

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

            # DCA 주문 ID 추적 (나중에 체결 시 수동 추가매수로 오분류 방지)
            if dca_order_ids:
                if symbol not in _pending_bot_dca_ids:
                    _pending_bot_dca_ids[symbol] = set()
                _pending_bot_dca_ids[symbol].update(dca_order_ids)
            logger.info(f"market_entry completed: {symbol}")
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

            # 미체결 주문 취소 (trailing stop 포함)
            await client.cancel_all_orders(symbol)
            _trailing_stop_active.discard(symbol)
            _known_sl_prices.pop(symbol, None)

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
            if success:
                _known_sl_prices[symbol] = str(request.sl_price)
            return {"success": success}

        elif request.order_type == "cancel_order":
            if not request.order_id:
                raise HTTPException(status_code=400, detail="order_id required")
            success = await client.cancel_order(symbol, request.order_id)
            return {"success": success}

        elif request.order_type == "cancel_all":
            success = await client.cancel_all_orders(symbol)
            _trailing_stop_active.discard(symbol)
            _known_sl_prices.pop(symbol, None)
            return {"success": success}

        elif request.order_type == "adjust":
            # 재조정: 기존 주문 전체 취소 → SL 설정 → TP/DCA 주문 재배치
            # trailing_stop_distance 없고 trailing stop 활성 상태면 track_plan 보존
            preserve_trailing = (request.trailing_stop_distance is None and symbol in _trailing_stop_active)
            await client.cancel_all_orders(symbol, cancel_trailing_stop=not preserve_trailing)
            if not preserve_trailing:
                _trailing_stop_active.discard(symbol)

            # 최신 포지션 조회 (DCA 체결로 인한 수량 변동 반영)
            position = await client.get_position(symbol)
            position_qty = position.qty if position else Decimal("0")

            sl_set = False
            if request.trailing_stop_distance is not None:
                if not position:
                    # 포지션이 이미 소멸 — SL 발동 등으로 청산됨, position-closed 콜백이 곧 전송될 것
                    logger.info(f"[adjust] position already gone, skipping trailing stop: {symbol}")
                    return {"success": True, "position_qty": "0", "sl_set": False, "tp_order_ids": [], "dca_order_ids": []}
                sl_set = await client.set_trailing_stop(symbol, request.trailing_stop_distance)
                if not sl_set:
                    # API 실패 — 백엔드가 Telegram 알림을 받을 수 있도록 명시적 실패 반환
                    logger.error(f"[adjust] trailing stop placement failed: {symbol}")
                    return {
                        "success": False,
                        "reason": "trailing_stop_failed",
                        "position_qty": str(position_qty),
                    }
                _trailing_stop_active.add(symbol)  # 성공 시 추적 등록
                _known_sl_prices.pop(symbol, None)  # trailing stop 전환 시 고정 SL 가격 제거
                # Bitget: trailing stop(track_plan)과 SL(pos_loss)이 독립 주문이므로 손익분기 SL 병행 설정
                if client.exchange_id == "bitget":
                    try:
                        await client.set_stop_loss(symbol, position.entry_price)
                        _known_sl_prices[symbol] = str(position.entry_price)  # breakeven SL 발동 시 MANUAL_CLOSE 오분류 방지
                        logger.info(f"[adjust] Bitget breakeven SL set @ {position.entry_price} alongside trailing stop: {symbol}")
                    except Exception as e:
                        logger.warning(f"[adjust] Bitget breakeven SL failed (trailing stop still active): {symbol} — {e}")
            elif request.sl_price:
                sl_set = await client.set_stop_loss(symbol, Decimal(str(request.sl_price)))
                if sl_set:
                    _known_sl_prices[symbol] = str(request.sl_price)
                else:
                    # set_stop_loss False = 포지션이 이미 소멸 (SL/수동 청산 race condition)
                    logger.info(f"[adjust] position already gone (SL not set), skipping TP/DCA: {symbol}")
                    return {"success": True, "position_qty": "0", "sl_set": False, "tp_order_ids": [], "dca_order_ids": []}

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

            # adjust에서 재배치된 DCA 주문 ID도 추적 (수동 추가매수 오분류 방지)
            if dca_order_ids:
                _pending_bot_dca_ids.setdefault(symbol, set()).update(dca_order_ids)

            logger.info(f"adjust completed: {symbol}")
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
        await _post_error_to_central("exchange_error", "main.py", f"execute:{request.order_type}", str(e))
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in execute [{request.order_type}]: {e}")
        await _post_error_to_central("unexpected", "main.py", f"execute:{request.order_type}", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ===== 공개 핑 (Railway 헬스체크용 — 인증 불필요) =====

@app.get("/healthz")
async def healthz():
    return {"status": "ok", "agent_variant": settings.agent_variant}


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
            "partner_code": settings.partner_code,
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
