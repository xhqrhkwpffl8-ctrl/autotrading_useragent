"""
Agent 전용 거래소 클라이언트 (standalone)
ccxt 기반 통합 거래소 클라이언트 - app 의존성 없는 독립 버전
"""

import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Optional, Dict, List

import ccxt.async_support as ccxt

logger = logging.getLogger("exchange_client")


class ExchangeError(Exception):
    """거래소 관련 오류"""
    pass


@dataclass
class Position:
    """포지션 정보"""
    symbol: str
    side: str       # "LONG" or "SHORT"
    qty: Decimal
    entry_price: Decimal
    leverage: int
    unrealized_pnl: Decimal = Decimal("0")
    stop_loss: Optional[Decimal] = None
    mark_price: Optional[Decimal] = None


class AgentExchangeClient:
    """ccxt 기반 통합 거래소 클라이언트 (Agent 전용)"""

    def __init__(self, exchange_id: str, api_key: str, api_secret: str, api_passphrase: str = ""):
        self.exchange_id = exchange_id.lower()
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase

        exchange_class = getattr(ccxt, self.exchange_id)
        exchange_config = {
            "apiKey": api_key,
            "secret": api_secret,
            "options": {
                "defaultType": "future",
                # Bybit: /v5/asset/coin/query-info 엔드포인트가 CloudFront 지역 차단됨
                # 선물 거래에 불필요한 currency 조회를 비활성화
                **({"fetchCurrencies": False} if exchange_id.lower() == "bybit" else {}),
            },
        }
        if api_passphrase:
            exchange_config["password"] = api_passphrase
        self.exchange: ccxt.Exchange = exchange_class(exchange_config)

        self._markets_loaded = False
        self._instrument_cache: Dict[str, Dict] = {}

    # 거래소별 심볼 별칭
    _EXCHANGE_SYMBOL_MAP: Dict[str, Dict[str, str]] = {
        "okx": {"XAUTUSDT": "XAUUSDT"},
        "binance": {"XAUTUSDT": "XAUUSDT"},
        "bitget": {"XAUTUSDT": "XAUUSDT"},
    }

    def _normalize_symbol(self, symbol: str) -> str:
        return self._EXCHANGE_SYMBOL_MAP.get(self.exchange_id, {}).get(symbol, symbol)

    def _to_ccxt_symbol(self, symbol: str) -> str:
        """'BTCUSDT' → 'BTC/USDT:USDT'"""
        symbol = self._normalize_symbol(symbol)
        if "/" in symbol:
            return symbol
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}/USDT:USDT"
        return symbol

    def _new_exchange(self) -> ccxt.Exchange:
        """신규 ccxt 인스턴스 생성 — Bitget 단방향 SL/TP용 (캐시된 posMode 오판 방지)"""
        exchange_class = getattr(ccxt, self.exchange_id)
        config: dict = {
            "apiKey": self._api_key,
            "secret": self._api_secret,
            "options": {"defaultType": "future"},
        }
        if self._api_passphrase:
            config["password"] = self._api_passphrase
        return exchange_class(config)

    def _log_known_exchange_error(self, e: Exception) -> bool:
        """알려진 거래소 오류 코드를 감지해 사용자 친화적 메시지 로깅. 알려진 오류면 True 반환."""
        err_str = str(e)
        if "40014" in err_str:
            logger.error(
                "[설정 필요] API 키 권한 오류 (40014): 거래소 웹사이트에서 "
                "선물 거래 권한이 포함된 API 키를 새로 발급하고 에이전트에 등록해 주세요."
            )
            return True
        if "43011" in err_str:
            logger.error(
                "[설정 필요] 포지션 모드 오류 (43011): 현재 Hedge(양방향) 모드로 설정되어 있습니다. "
                "거래소 설정에서 One-Way(단방향) 모드로 변경 후 다시 시도해 주세요."
            )
            return True
        return False

    async def _ensure_markets(self):
        if not self._markets_loaded:
            await self.exchange.load_markets()
            self._markets_loaded = True

    def get_instrument_info(self, symbol: str) -> Dict:
        if symbol in self._instrument_cache:
            return self._instrument_cache[symbol]
        if self._markets_loaded:
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            try:
                market = self.exchange.market(ccxt_symbol)
                precision = market.get("precision", {})
                limits = market.get("limits", {})
                amount_limits = limits.get("amount", {})
                cost_limits = limits.get("cost", {})

                qty_step_raw = precision.get("amount")
                tick_size_raw = precision.get("price")

                qty_step = Decimal(str(qty_step_raw)) if qty_step_raw else Decimal("0.001")
                tick_size = Decimal(str(tick_size_raw)) if tick_size_raw else Decimal("0.01")
                min_qty = Decimal(str(amount_limits.get("min") or "0.001"))
                min_amt = Decimal(str(cost_limits.get("min") or "5"))

                info = {
                    "qtyStep": qty_step,
                    "tickSize": tick_size,
                    "minOrderQty": min_qty,
                    "minOrderAmt": min_amt,
                }
                self._instrument_cache[symbol] = info
                return info
            except Exception as e:
                logger.warning(f"Failed to get market info for {symbol}: {e}")
        return {
            "qtyStep": Decimal("0.001"),
            "tickSize": Decimal("0.01"),
            "minOrderQty": Decimal("0.001"),
            "minOrderAmt": Decimal("5"),
        }

    def round_quantity(self, symbol: str, qty: Decimal) -> Decimal:
        info = self.get_instrument_info(symbol)
        qty_step = info["qtyStep"]
        return (qty / qty_step).quantize(Decimal("1"), rounding=ROUND_DOWN) * qty_step

    @staticmethod
    def _safe_tick_for_price(price: Decimal) -> Decimal:
        """가격 기반 tick size 추정 (ccxt 데이터 없을 때 fallback용)"""
        if price < Decimal("0.0001"):   return Decimal("0.00000001")  # SHIB 등
        elif price < Decimal("0.01"):   return Decimal("0.000001")
        elif price < Decimal("0.1"):    return Decimal("0.00001")     # DOGE
        elif price < Decimal("10"):     return Decimal("0.0001")
        elif price < Decimal("100"):    return Decimal("0.001")
        elif price < Decimal("10000"):  return Decimal("0.01")        # ETH
        else:                           return Decimal("0.1")          # BTC

    def round_price(self, symbol: str, price: Decimal) -> Decimal:
        info = self.get_instrument_info(symbol)
        tick = info["tickSize"]
        # 안전 검사: tick이 가격의 10% 초과 → 잘못된 tick (fallback 오염)
        if tick > price * Decimal("0.1"):
            safe_tick = self._safe_tick_for_price(price)
            logger.warning(
                f"[{symbol}] tick={tick} seems wrong for price={price}, "
                f"using estimated tick={safe_tick}"
            )
            tick = safe_tick
        return (price / tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * tick

    # ===== 잔고 조회 =====

    async def get_balance(self) -> Optional[Decimal]:
        try:
            await self._ensure_markets()
            balance = await self.exchange.fetch_balance()
            usdt = balance.get("USDT", {})
            free = usdt.get("free", 0) or 0
            return Decimal(str(free))
        except Exception as e:
            self._log_known_exchange_error(e)
            logger.error(f"Failed to get balance: {e}")
            return None

    async def get_account_uid(self) -> Optional[str]:
        """API key에 연결된 거래소 계정 UID 조회 (레퍼럴 검증용)"""
        try:
            if self.exchange_id == "bybit":
                resp = await self.exchange.private_get_v5_user_query_api()
                return str(resp["result"]["userID"])
            elif self.exchange_id == "okx":
                resp = await self.exchange.private_get_account_config()
                return str(resp["data"][0]["uid"])
            elif self.exchange_id == "bitget":
                resp = await self.exchange.private_spot_get_v2_spot_account_info()
                return str(resp["data"]["userId"])
            elif self.exchange_id == "bingx":
                resp = await self.exchange.private_get_openapi_account_v1_uid()
                return str(resp["data"]["uid"])
            return None
        except Exception as e:
            self._log_known_exchange_error(e)
            logger.error(f"get_account_uid failed [{self.exchange_id}]")
            return None

    # ===== 현재가 조회 =====

    async def get_current_price(self, symbol: str) -> Optional[Decimal]:
        try:
            await self._ensure_markets()
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            ticker = await self.exchange.fetch_ticker(ccxt_symbol)
            last = ticker.get("last")
            return Decimal(str(last)) if last else None
        except Exception as e:
            logger.error(f"Failed to get current price for {symbol}: {e}")
            return None

    # ===== 포지션 조회 =====

    async def get_position(self, symbol: str) -> Optional[Position]:
        try:
            await self._ensure_markets()
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            positions = await self.exchange.fetch_positions([ccxt_symbol])
            for pos in positions:
                contracts = Decimal(str(pos.get("contracts") or 0))
                if contracts <= 0:
                    continue
                side = "LONG" if pos["side"] == "long" else "SHORT"
                sl_raw = pos.get("stopLossPrice")
                mark_raw = pos.get("markPrice")
                return Position(
                    symbol=symbol,
                    side=side,
                    qty=contracts,
                    entry_price=Decimal(str(pos["entryPrice"])),
                    leverage=int(pos.get("leverage") or 1),
                    unrealized_pnl=Decimal(str(pos.get("unrealizedPnl") or 0)),
                    stop_loss=Decimal(str(sl_raw)) if sl_raw else None,
                    mark_price=Decimal(str(mark_raw)) if mark_raw else None,
                )
            return None
        except Exception as e:
            logger.error(f"Failed to get position: {e}")
            raise ExchangeError(f"get_position failed for {symbol}: {e}")

    async def get_all_positions(self) -> List[Dict]:
        try:
            await self._ensure_markets()
            positions = await self.exchange.fetch_positions()
            return [p for p in positions if Decimal(str(p.get("contracts") or 0)) > 0]
        except Exception as e:
            logger.error(f"Failed to get all positions: {e}")
            return []

    # ===== 포지션 모드 / 레버리지 =====

    async def switch_to_one_way_mode(self, symbol: str) -> bool:
        try:
            await self._ensure_markets()
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            if self.exchange_id == "bybit":
                await self.exchange.set_position_mode(False, ccxt_symbol)
            elif hasattr(self.exchange, "set_position_mode"):
                await self.exchange.set_position_mode(False, ccxt_symbol)
            logger.info(f"One-way mode set for {symbol}")
            return True
        except Exception as e:
            if "not modified" in str(e).lower() or "already" in str(e).lower():
                return True
            self._log_known_exchange_error(e)
            logger.error(f"Failed to switch to one-way mode: {e}")
            return False

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        try:
            await self._ensure_markets()
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            await self.exchange.set_leverage(leverage, ccxt_symbol)
            logger.info(f"Leverage set: {leverage}x for {symbol}")
            return True
        except Exception as e:
            if "110043" in str(e):
                return True
            logger.error(f"Failed to set leverage: {e}")
            return False

    # ===== 주문 실행 =====

    async def place_market_order(self, symbol: str, side: str, qty: Decimal) -> Optional[str]:
        try:
            await self._ensure_markets()
            rounded_qty = self.round_quantity(symbol, qty)
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            ccxt_side = side.lower()
            order = await self.exchange.create_order(
                ccxt_symbol, "market", ccxt_side, float(rounded_qty)
            )
            order_id = order["id"]
            logger.info(f"Market order placed: {side} {rounded_qty} {symbol} (ID: {order_id})")
            return order_id
        except Exception as e:
            self._log_known_exchange_error(e)
            logger.error(f"Failed to place market order: {e}")
            raise ExchangeError(f"place_market_order failed for {symbol}: {e}")

    async def place_limit_order(self, symbol: str, side: str, qty: Decimal, price: Decimal) -> Optional[str]:
        try:
            await self._ensure_markets()
            rounded_qty = self.round_quantity(symbol, qty)
            rounded_price = self.round_price(symbol, price)
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            order = await self.exchange.create_order(
                ccxt_symbol, "limit", side.lower(), float(rounded_qty), float(rounded_price)
            )
            order_id = order["id"]
            logger.info(f"Limit order placed: {side} {rounded_qty} {symbol} @ {rounded_price} (ID: {order_id})")
            return order_id
        except Exception as e:
            logger.error(f"Failed to place limit order: {e}")
            raise ExchangeError(f"place_limit_order failed for {symbol}: {e}")

    async def place_tp_order(self, symbol: str, side: str, qty: Decimal, price: Decimal) -> Optional[str]:
        try:
            await self._ensure_markets()
            rounded_qty = self.round_quantity(symbol, qty)
            rounded_price = self.round_price(symbol, price)
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            if self.exchange_id == "bitget":
                # 단방향 모드 TP: holdSide를 진입 방향("buy" 또는 "sell")으로 설정 (단방향 포지션 필수)
                import time
                hold_side = "buy" if side.lower() == "sell" else "sell"
                resp = await self.exchange.private_mix_post_v2_mix_order_place_tpsl_order({
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "marginMode": "crossed",
                    "marginCoin": "USDT",
                    "planType": "profit_plan",
                    "triggerPrice": str(rounded_price),
                    "triggerType": "fill_price",
                    "executePrice": str(rounded_price),
                    "size": str(rounded_qty),
                    "holdSide": hold_side,
                    "delegateType": "limit",
                })
                order_id = resp.get("data", {}).get("orderId", f"bitget_tp_{int(time.time())}")
                logger.info(f"TP plan order placed: {side} {rounded_qty} {symbol} @ {rounded_price} (ID: {order_id})")
                return order_id
            else:
                order = await self.exchange.create_order(
                    ccxt_symbol, "limit", side.lower(), float(rounded_qty), float(rounded_price),
                    {"reduceOnly": True, "timeInForce": "GTC"}
                )
                order_id = order["id"]
                logger.info(f"TP order placed: {side} {rounded_qty} {symbol} @ {rounded_price} (ID: {order_id})")
                return order_id
        except Exception as e:
            err_str = str(e)
            if "43023" in err_str:
                logger.warning(f"No position available for TP order (position already closed): {symbol}")
                return None
            logger.error(f"Failed to place TP order: {e}")
            raise ExchangeError(f"place_tp_order failed for {symbol}: {e}")

    async def set_stop_loss(self, symbol: str, stop_loss_price: Decimal) -> bool:
        try:
            await self._ensure_markets()
            rounded_sl = self.round_price(symbol, stop_loss_price)
            if self.exchange_id == "bybit":
                position = await self.get_position(symbol)
                if not position:
                    logger.warning(f"No position found to set SL for {symbol}")
                    return False
                check_price = position.mark_price or position.entry_price
                if position.side == "SHORT" and rounded_sl <= check_price:
                    raise ExchangeError(
                        f"set_stop_loss failed for {symbol}: SHORT 포지션의 SL({rounded_sl})은 "
                        f"현재가({check_price})보다 높아야 합니다"
                    )
                if position.side == "LONG" and rounded_sl >= check_price:
                    raise ExchangeError(
                        f"set_stop_loss failed for {symbol}: LONG 포지션의 SL({rounded_sl})은 "
                        f"현재가({check_price})보다 낮아야 합니다"
                    )
                await self.exchange.private_post_v5_position_trading_stop({
                    "category": "linear",
                    "symbol": symbol,
                    "stopLoss": str(rounded_sl),
                    "trailingStop": "0",   # 기존 트레일링 스탑 클리어 (재조정 시 충돌 방지)
                    "slTriggerBy": "LastPrice",
                    "positionIdx": 0,
                })
            elif self.exchange_id == "bitget":
                position = await self.get_position(symbol)
                if not position:
                    logger.warning(f"No position found to set SL for {symbol}")
                    return False
                # Bitget validates SL against current mark price (not entry price).
                # Pre-validate to avoid error 40834 and surface a clear message.
                if position.mark_price:
                    if position.side == "LONG" and rounded_sl >= position.mark_price:
                        raise ExchangeError(
                            f"set_stop_loss failed for {symbol}: LONG SL({rounded_sl}) >= "
                            f"mark_price({position.mark_price}) — price dropped past SL"
                        )
                    if position.side == "SHORT" and rounded_sl <= position.mark_price:
                        raise ExchangeError(
                            f"set_stop_loss failed for {symbol}: SHORT SL({rounded_sl}) <= "
                            f"mark_price({position.mark_price}) — price rose past SL"
                        )
                hold_side = "buy" if position.side == "LONG" else "sell"
                await self.exchange.private_mix_post_v2_mix_order_place_tpsl_order({
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "marginMode": "crossed",
                    "marginCoin": "USDT",
                    "planType": "pos_loss",
                    "triggerPrice": str(rounded_sl),
                    "triggerType": "fill_price",
                    "size": str(position.qty),
                    "holdSide": hold_side,
                    "delegateType": "market",
                })
            else:
                ccxt_symbol = self._to_ccxt_symbol(symbol)
                position = await self.get_position(symbol)
                if not position:
                    logger.warning(f"No position found to set SL for {symbol}")
                    return False
                close_side = "sell" if position.side == "LONG" else "buy"
                await self.exchange.create_order(
                    ccxt_symbol, "stop_market", close_side, float(position.qty), None,
                    {"stopPrice": float(rounded_sl), "reduceOnly": True}
                )
            logger.info(f"Stop loss set: {symbol} @ {rounded_sl}")
            return True
        except Exception as e:
            err_str = str(e)
            if "34040" in err_str or "not modified" in err_str.lower():
                logger.info(f"Stop loss already set at same price for {symbol} @ {stop_loss_price}")
                return True
            logger.error(f"Failed to set stop loss for {symbol}: {e}")
            if "40834" in err_str:
                # LONG: SL must be below current price
                raise ExchangeError(f"set_stop_loss failed for {symbol}: SL({stop_loss_price})이 현재가보다 높습니다 (LONG 포지션)")
            if "40835" in err_str:
                # SHORT: SL must be above current price
                raise ExchangeError(f"set_stop_loss failed for {symbol}: SL({stop_loss_price})이 현재가보다 낮습니다 (SHORT 포지션)")
            raise ExchangeError(f"set_stop_loss failed for {symbol}: {e}")

    async def set_trailing_stop(self, symbol: str, trailing_distance: float) -> bool:
        """트레일링 스탑 설정. Bybit: USDT 거리(최소 0.5%), Bitget: callbackRatio(%) 자동 변환(최소 0.5%)."""
        try:
            await self._ensure_markets()
            if self.exchange_id == "bybit":
                position = await self.get_position(symbol)
                if position:
                    mark_price = float(position.mark_price or position.entry_price)
                    min_distance = round(mark_price * 0.005, 1)
                    if trailing_distance < min_distance:
                        logger.warning(
                            f"[TrailingStop] distance {trailing_distance} < 0.5% min {min_distance} → using min"
                        )
                        trailing_distance = min_distance
                await self.exchange.private_post_v5_position_trading_stop({
                    "category": "linear",
                    "symbol": symbol,
                    "stopLoss": "0",                                # 기존 고정 SL 클리어
                    "trailingStop": str(round(trailing_distance, 1)),
                    "positionIdx": 0,
                })
            elif self.exchange_id == "bitget":
                position = await self.get_position(symbol)
                if not position:
                    logger.warning(f"[TrailingStop] No position found for {symbol}")
                    return False
                close_side = "sell" if position.side == "LONG" else "buy"
                mark_price = float(position.mark_price or position.entry_price)
                callback_ratio = round(trailing_distance / mark_price * 100, 4)
                if callback_ratio < 0.5:
                    logger.warning(
                        f"[TrailingStop] callback_ratio {callback_ratio}% < 0.5% min → using 0.5%"
                    )
                    callback_ratio = 0.5
                # triggerPrice: Bitget track_plan 활성화 조건은 mark_price >= triggerPrice(SELL) / mark_price <= triggerPrice(BUY)
                # 즉시 활성화하려면 이미 통과한 가격으로 설정 (SELL: 현재가 1% 아래, BUY: 현재가 1% 위)
                if close_side == "sell":
                    trigger_price = round(mark_price * 0.99, 1)  # LONG 청산: 현재가 아래 → 즉시 활성
                else:
                    trigger_price = round(mark_price * 1.01, 1)  # SHORT 청산: 현재가 위 → 즉시 활성
                resp = await self.exchange.private_mix_post_v2_mix_order_place_plan_order({
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "marginMode": "crossed",
                    "marginCoin": "USDT",
                    "planType": "track_plan",
                    "triggerPrice": str(trigger_price),
                    "triggerType": "mark_price",
                    "callbackRatio": str(callback_ratio),
                    "size": str(position.qty),
                    "side": close_side,
                    "orderType": "market",
                })
                order_id = (resp.get("data") or {}).get("orderId", "unknown")
                logger.info(
                    f"[TrailingStop] Bitget track_plan placed: orderId={order_id}, "
                    f"triggerPrice={trigger_price}, callbackRatio={callback_ratio}%, "
                    f"side={close_side}, size={position.qty}"
                )
            else:
                logger.warning(f"[TrailingStop] {self.exchange_id} 미지원 — 스킵")
                return False
            logger.info(f"Trailing stop set: {symbol}, distance={trailing_distance}")
            return True
        except Exception as e:
            logger.error(f"Failed to set trailing stop for {symbol}: {e}")
            return False

    async def has_pending_trailing_stop(self, symbol: str) -> bool:
        """Bitget open plan orders에서 track_plan(trailing stop)이 pending 상태인지 확인.
        trailing stop 발동 여부 판별용 — 발동했으면 open orders에 없음(False), 미발동이면 있음(True).
        비-Bitget 또는 조회 실패 시 True 반환(보수적: trailing stop 발동 아닌 것으로 간주).
        → 호출부: trailing_stop_fired = not has_pending_trailing_stop()"""
        if self.exchange_id != "bitget":
            return True  # 비-Bitget: 판별 불가 → 보수적으로 pending(=발동 아님) 처리
        try:
            resp = await self.exchange.private_mix_get_v2_mix_order_orders_plan_pending({
                "symbol": symbol,
                "productType": "USDT-FUTURES",
                "planType": "track_plan",
            })
            orders = (resp.get("data") or {}).get("entrustedList") or []
            return len(orders) > 0
        except Exception as e:
            logger.warning(f"[TrailingStop] open plan orders 조회 실패 (보수적 처리): {e}")
            return True  # 조회 실패 시 trailing stop 발동 아닌 것으로 간주

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        try:
            await self._ensure_markets()
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            await self.exchange.cancel_order(order_id, ccxt_symbol)
            logger.info(f"Order {order_id} cancelled")
            return True
        except Exception as e:
            if "110001" in str(e):
                return True
            logger.error(f"Failed to cancel order {order_id}: {e}")
            raise ExchangeError(f"cancel_order failed for {order_id}: {e}")

    async def cancel_all_orders(self, symbol: str, cancel_trailing_stop: bool = True) -> bool:
        try:
            await self._ensure_markets()
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            try:
                await self.exchange.cancel_all_orders(ccxt_symbol)
            except Exception as e:
                # 22001 (No order to cancel) 등 에러는 무시
                if "22001" not in str(e) and "No order" not in str(e):
                    logger.warning(f"Failed to cancel regular orders for {symbol}: {e}")

            if self.exchange_id == "bitget":
                # ccxt cancel_all_orders는 일반 주문만 취소 — plan(stop/TP) 주문은 네이티브 API로 일괄 취소
                # cancel_trailing_stop=False 시 track_plan(trailing stop)은 보존
                if cancel_trailing_stop:
                    try:
                        await self.exchange.private_mix_post_v2_mix_order_cancel_plan_order({
                            "symbol": symbol,
                            "productType": "USDT-FUTURES",
                            "marginCoin": "USDT",
                        })
                        logger.info(f"All plan orders batch-cancelled natively for {symbol}")
                    except Exception as ce:
                        # 22001 (취소할 주문 없음), 400172 (파라미터 에러 - 플랜타입 필요시 대비) 무시
                        if "22001" not in str(ce) and "400171" not in str(ce):
                            logger.warning(f"Failed to batch cancel plan orders for {symbol}: {ce}")

                # 400172 방어를 위해 각 planType 명시적 일괄 취소도 병행
                plan_types = ["profit_plan", "pos_loss", "pos_profit", "normal_plan"]
                if cancel_trailing_stop:
                    plan_types.append("track_plan")
                for p_type in plan_types:
                    try:
                        await self.exchange.private_mix_post_v2_mix_order_cancel_plan_order({
                            "symbol": symbol,
                            "productType": "USDT-FUTURES",
                            "marginCoin": "USDT",
                            "planType": p_type
                        })
                    except Exception:
                        pass


            logger.info(f"All open orders cancelled for {symbol}")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel all orders for {symbol}: {e}")
            return False

    async def close_position(self, symbol: str, side: str) -> bool:
        try:
            await self._ensure_markets()
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            close_side = "sell" if side == "LONG" else "buy"
            position = await self.get_position(symbol)
            if not position:
                logger.warning(f"No position to close for {symbol}")
                return False
            order = await self.exchange.create_order(
                ccxt_symbol, "market", close_side, float(position.qty),
                None, {"reduceOnly": True}
            )
            logger.info(f"Position closed: {side} {symbol} (ID: {order['id']})")
            return True
        except Exception as e:
            logger.error(f"Failed to close position: {e}")
            raise ExchangeError(f"close_position failed for {symbol}: {e}")

    def _to_okx_inst_id(self, symbol: str) -> str:
        """'BTCUSDT' → 'BTC-USDT-SWAP'"""
        symbol = self._normalize_symbol(symbol)
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}-USDT-SWAP"
        return symbol

    def _to_bingx_symbol(self, symbol: str) -> str:
        """'BTCUSDT' → 'BTC-USDT'"""
        symbol = self._normalize_symbol(symbol)
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}-USDT"
        return symbol

    async def get_closed_pnl(self, symbol: str, limit: int = 1) -> Optional[Dict]:
        """마지막 청산 포지션의 PnL 정보 조회 (다거래소 지원)"""
        try:
            if self.exchange_id == "bybit":
                result = await self.exchange.private_get_v5_position_closed_pnl({
                    "category": "linear",
                    "symbol": symbol,
                    "limit": limit,
                })
                rows = result.get("result", {}).get("list", [])
                return rows[0] if rows else None

            elif self.exchange_id == "okx":
                result = await self.exchange.private_get_account_positions_history({
                    "instId": self._to_okx_inst_id(symbol),
                    "limit": limit,
                })
                rows = result.get("data", [])
                if not rows:
                    return None
                row = rows[0]
                return {
                    "avgExitPrice": row.get("closeAvgPx"),
                    "closedPnl": row.get("realizedPnl"),
                    "createdTime": row.get("uTime"),
                }

            elif self.exchange_id == "bitget":
                result = await self.exchange.private_mix_get_v2_mix_position_history_position({
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                    "limit": limit,
                })
                rows = result.get("data", {}).get("list", [])
                if not rows:
                    return None
                row = rows[0]
                return {
                    "avgExitPrice": row.get("closeAvgPrice"),
                    "closedPnl": row.get("totalPnl"),
                    "createdTime": row.get("utime"),
                }

            elif self.exchange_id == "bingx":
                result = await self.exchange.swap_v1_private_get_trade_position_history({
                    "symbol": self._to_bingx_symbol(symbol),
                    "pageSize": limit,
                })
                rows = result.get("data", {}).get("positionHistory", [])
                if not rows:
                    return None
                row = rows[0]
                return {
                    "avgExitPrice": row.get("avgClosePrice"),
                    "closedPnl": row.get("realisedProfit"),
                    "createdTime": row.get("updateTime"),
                }

            return None
        except Exception as e:
            logger.warning(f"get_closed_pnl failed for {symbol}: {e}")
            return None

    async def get_recent_fills(self, symbol: str, limit: int = 5) -> List[Dict]:
        """최근 체결 내역 조회 (실제 체결가 확인용)"""
        try:
            if self.exchange_id == "bybit":
                result = await self.exchange.private_get_v5_execution_list({
                    "category": "linear",
                    "symbol": symbol,
                    "limit": limit,
                })
                return result.get("result", {}).get("list", [])
            else:
                ccxt_symbol = self._to_ccxt_symbol(symbol)
                trades = await self.exchange.fetch_my_trades(ccxt_symbol, limit=limit)
                return [
                    {
                        "execPrice": str(t["price"]),
                        "execQty": str(t["amount"]),
                        "execTime": str(int(t["timestamp"])),
                        # ccxt "limit"/"market" → capitalize → "Limit"/"Market" (Bybit raw 포맷과 통일)
                        "orderType": (t.get("type") or "").capitalize(),
                        "execFee": str(t.get("fee", {}).get("cost") or 0),
                    }
                    for t in trades
                ]
        except Exception as e:
            logger.warning(f"get_recent_fills failed for {symbol}: {e}")
            return []

    async def close(self):
        try:
            await self.exchange.close()
        except Exception:
            pass
        finally:
            try:
                self.exchange.apiKey = ""
                self.exchange.secret = ""
                self.exchange.password = ""
            except Exception:
                pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
