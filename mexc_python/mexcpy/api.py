import dataclasses
from enum import Enum
from dataclasses import asdict, dataclass
import types
from typing import Any, Dict, List, Optional, Type, TypeVar, Generic, Union
import aiohttp

from .sign import get_data
from .mexcTypes import (
    AssetInfo, OrderId, TransferRecords, PositionInfo, FundingRecords, Order, Transaction,
    TriggerOrder, StopLimitOrder, RiskLimit, TradingFeeInfo, Leverage, PositionMode,
    CreateOrderRequest, TriggerOrderRequest, ExecuteCycle, PositionType, OpenType,
    OrderSide, OrderType, OrderCategory, TriggerType, TriggerPriceType,
    PositionSide
)


def asdict_factory_with_enum_support(data):
    def convert_value(obj):
        return obj.value if isinstance(obj, Enum) else obj
    return dict((k, convert_value(v)) for k, v in data)

T = TypeVar('T')

@dataclass
class ApiResponse(Generic[T]):
    """A generic API response structure."""
    success: bool
    code: int
    data: T
    message: Optional[str] = None

    @classmethod
    def from_dict(cls, data_dict: Dict[str, Any], data_type: Type[T]) -> 'ApiResponse[T]':
        """
        Creates an ApiResponse instance from a dictionary, ignoring extra fields
        when instantiating known dataclasses.
        """
        processed_data: Any = None
        raw_data = data_dict.get('data')

        if raw_data is None:
            processed_data = None
        elif isinstance(raw_data, dict):
            if data_type is dict or data_type is Any:
                processed_data = types.SimpleNamespace(**raw_data)
            else:
                if dataclasses.is_dataclass(data_type):
                    expected_keys = {f.name for f in dataclasses.fields(data_type)}
                    filtered_data = {
                        k: v for k, v in raw_data.items() if k in expected_keys
                    }
                    try:
                        processed_data = data_type(**filtered_data) # type: ignore
                    except TypeError as e:
                        # Handle potential errors even after filtering (e.g., type mismatch)
                        print(f"Warning: Could not instantiate {data_type} even after filtering: {e}")
                        processed_data = filtered_data # Fallback to filtered dict
                else:
                    try:
                        processed_data = data_type(**raw_data) # type: ignore
                    except TypeError as e:
                        if 'unexpected keyword argument' in str(e):
                             print(f"Warning: Ignoring extra fields for non-dataclass {data_type}. Error: {e}")
                             processed_data = raw_data
                        else:
                             raise e
        elif isinstance(raw_data, list):
            processed_data = []
            for item in raw_data:
                if isinstance(item, dict):
                    if data_type is dict or data_type is Any:
                        processed_data.append(types.SimpleNamespace(**item))
                    else:
                        if dataclasses.is_dataclass(data_type):
                            expected_keys = {f.name for f in dataclasses.fields(data_type)}
                            filtered_item = {k: v for k, v in item.items() if k in expected_keys}
                            try:
                                processed_data.append(data_type(**filtered_item)) # type: ignore
                            except TypeError as e:
                                print(f"Warning: Could not instantiate list item {data_type}: {e}")
                                processed_data.append(filtered_item)
                        else:
                             try:
                                  processed_data.append(data_type(**item)) # type: ignore
                             except TypeError as e:
                                  if 'unexpected keyword argument' in str(e):
                                       print(f"Warning: Ignoring extra fields for list item {data_type}. Error: {e}")
                                       processed_data.append(item)
                                  else:
                                       raise e
                else:
                    processed_data.append(item)
        else:
            processed_data = raw_data

        return cls(
            success=data_dict.get('success', False),
            code=data_dict.get('code', 0),
            data=processed_data,
            message=data_dict.get('message')
        )

class MexcFuturesAPI:
    def __init__(self, token: str, testnet: bool = False):
        self.token = token
        self.base_url = (
            "https://futures.testnet.mexc.com/api/v1" 
            if testnet 
            else "https://futures.mexc.com/api/v1"
        )
        self.user_agent = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/json",
            "Language": "English",
            "x-mxc-sign": None,
            "x-mxc-nonce": None,
            "Authorization": None,
            "Pragma": "akamai-x-cache-on",
            "Origin": "https://futures.mexc.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "TE": "trailers",
        }

    async def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Union[Dict[str, Any], List[Any]]] = None,
        response_type: Optional[Type[T]] = None
    ) -> ApiResponse[T]:
        url_params = ""
        if method == "GET" and data is not None:
            url_params = f"?{self._dict_to_url_params(data)}" # type: ignore
        
        signed_data, sign, ts = get_data(data, self.token)
        
        body_data = (
            signed_data 
            if data is not None and isinstance(data, dict) and not isinstance(data, list)
            else data
        )

        headers = {
            **self.user_agent,
            "x-mxc-sign": sign,
            "x-mxc-nonce": ts,
            "Authorization": self.token,
        }

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method,
                f"{self.base_url}{endpoint}{url_params}",
                headers=headers,
                json=body_data if method == "POST" else None,
            ) as response:
                response_data = await response.json()
                
                if response_type:
                    return ApiResponse.from_dict(response_data, response_type)
                
                return ApiResponse(
                    success=response_data.get("success", False),
                    code=response_data.get("code", 0),
                    data=response_data.get("data"),
                    message=response_data.get("message"),
                )


    def _dict_to_url_params(self, params: Dict[str, Any]) -> str:
        return "&".join(f"{k}={v}" for k, v in params.items() if v is not None)

    # Account endpoints
    async def get_user_assets(self) -> ApiResponse[List[AssetInfo]]:
        return await self._make_request("GET", "/private/account/assets")

    async def get_user_asset(self, currency: str) -> ApiResponse[AssetInfo]:
        return await self._make_request("GET", f"/private/account/asset/{currency}", response_type=AssetInfo)

    async def get_asset_transfer_records(
        self,
        currency: Optional[str] = None,
        state: Optional[str] = None,
        type: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> ApiResponse[TransferRecords]:
        params = {
            "currency": currency,
            "state": state,
            "type": type,
            "page_num": page_num,
            "page_size": page_size,
        }
        return await self._make_request("GET", "/private/account/transfer_record", params, response_type=TransferRecords)

    # Position endpoints
    async def get_historical_positions(
        self,
        symbol: Optional[str] = None,
        position_type: Optional[PositionType] = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> ApiResponse[List[PositionInfo]]:
        params = {
            "symbol": symbol,
            "type": position_type.value if position_type else None,
            "page_num": page_num,
            "page_size": page_size,
        }
        return await self._make_request(
            "GET", "/private/position/list/history_positions", params
        )

    async def get_open_positions(
        self, symbol: Optional[str] = None
    ) -> ApiResponse[List[PositionInfo]]:
        params = {"symbol": symbol} if symbol else {}
        return await self._make_request("GET", "/private/position/open_positions", params)

    async def get_funding_records(
        self,
        symbol: Optional[str] = None,
        position_id: Optional[int] = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> ApiResponse[FundingRecords]:
        params = {
            "symbol": symbol,
            "position_id": position_id,
            "page_num": page_num,
            "page_size": page_size,
        }
        return await self._make_request(
            "GET", "/private/position/funding_records", params, response_type=FundingRecords
        )

    # Order endpoints
    async def get_current_pending_orders(
        self, symbol: Optional[str] = None, page_num: int = 1, page_size: int = 20
    ) -> ApiResponse[List[Order]]:
        params = {
            "symbol": symbol,
            "page_num": page_num,
            "page_size": page_size,
        }
        return await self._make_request("GET", "/private/order/list/open_orders", params)

    async def get_historical_orders(
        self,
        symbol: Optional[str] = None,
        states: Optional[str] = None,
        category: Optional[OrderCategory] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        side: Optional[OrderSide] = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> ApiResponse[List[Order]]:
        params = {
            "symbol": symbol,
            "states": states,
            "category": category.value if category else None,
            "start_time": start_time,
            "end_time": end_time,
            "side": side.value if side else None,
            "page_num": page_num,
            "page_size": page_size,
        }
        return await self._make_request("GET", "/private/order/list/history_orders", params)

    async def get_order_by_external_oid(
        self, symbol: str, external_oid: str
    ) -> ApiResponse[Order]:
        return await self._make_request(
            "GET", f"/private/order/external/{symbol}/{external_oid}", response_type=Order
        )

    async def get_order_by_order_id(self, order_id: str) -> ApiResponse[Order]:
        return await self._make_request("GET", f"/private/order/get/{order_id}", response_type=Order)

    async def get_orders_by_order_ids(
        self, order_ids: List[str]
    ) -> ApiResponse[List[Order]]:
        return await self._make_request(
            "GET", "/private/order/batch_query", {"order_ids": order_ids}
        )

    async def get_order_transactions(self, order_id: str) -> ApiResponse[List[Transaction]]:
        return await self._make_request(
            "GET", f"/private/order/deal_details/{order_id}"
        )

    async def get_order_transactions_by_symbol(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> ApiResponse[List[Transaction]]:
        params = {
            "symbol": symbol,
            "start_time": start_time,
            "end_time": end_time,
            "page_num": page_num,
            "page_size": page_size,
        }
        return await self._make_request("GET", "/private/order/list/order_deals", params)

    # Trigger order endpoints
    async def get_trigger_orders(
        self,
        symbol: Optional[str] = None,
        states: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> ApiResponse[List[TriggerOrder]]:
        params = {
            "symbol": symbol,
            "states": states,
            "start_time": start_time,
            "end_time": end_time,
            "page_num": page_num,
            "page_size": page_size,
        }
        return await self._make_request("GET", "/private/planorder/list/orders", params)

    async def get_stop_limit_orders(
        self,
        symbol: Optional[str] = None,
        is_finished: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> ApiResponse[List[StopLimitOrder]]:
        params = {
            "symbol": symbol,
            "is_finished": is_finished,
            "start_time": start_time,
            "end_time": end_time,
            "page_num": page_num,
            "page_size": page_size,
        }
        return await self._make_request("GET", "/private/stoporder/list/orders", params)

    # Risk management endpoints
    async def get_risk_limits(self, symbol: Optional[str] = None) -> ApiResponse[RiskLimit]:
        params = {"symbol": symbol} if symbol else {}
        return await self._make_request("GET", "/private/account/risk_limit", params, response_type=RiskLimit)

    async def get_user_trading_fee(self, symbol: str) -> ApiResponse[TradingFeeInfo]:
        return await self._make_request(
            "GET", "/private/account/tiered_fee_rate", {"symbol": symbol}, response_type=TradingFeeInfo
        )

    async def change_margin(
        self, position_id: int, amount: float, margin_type: str
    ) -> ApiResponse[None]:
        if margin_type not in ["ADD", "SUB"]:
            raise ValueError("margin_type must be either 'ADD' or 'SUB'")
        params = {
            "positionId": position_id,
            "amount": amount,
            "type": margin_type,
        }
        return await self._make_request("POST", "/private/position/change_margin", params)

    async def get_leverage(self, symbol: str) -> ApiResponse[List[Leverage]]:
        return await self._make_request(
            "GET", "/private/position/leverage", {"symbol": symbol}
        )

    async def change_leverage(
        self,
        leverage: int,
        position_id: Optional[int] = None,
        symbol: Optional[str] = None,
        open_type: Optional[OpenType] = None,
        position_type: Optional[PositionSide] = None,
    ) -> ApiResponse[None]:
        if position_id is not None:
            params = {
                "positionId": position_id,
                "leverage": leverage,
            }
        else:
            if not all([symbol, open_type, position_type]):
                raise ValueError(
                    "When position_id is not provided, symbol, open_type and position_type must be provided"
                )
            params = {
                "symbol": symbol,
                "leverage": leverage,
                "openType": open_type.value, # type: ignore
                "positionType": position_type.value, # type: ignore
            }
        return await self._make_request("POST", "/private/position/change_leverage", params)

    async def get_position_mode(self) -> ApiResponse[PositionMode]:
        return await self._make_request("GET", "/private/position/position_mode", response_type=PositionMode)

    async def change_position_mode(self, position_mode: PositionMode) -> ApiResponse[None]:
        return await self._make_request(
            "POST",
            "/private/position/change_position_mode",
            {"positionMode": position_mode.value},
        )

    # Order management
    async def create_order(self, order_request: CreateOrderRequest) -> ApiResponse[OrderId]:
        return await self._make_request(
            "POST", "/private/order/create", asdict(order_request, dict_factory=asdict_factory_with_enum_support), response_type=OrderId
        )

    async def cancel_orders(self, order_ids: List[str]) -> ApiResponse[List[Dict[str, Any]]]:
        return await self._make_request("POST", "/private/order/cancel", order_ids)

    async def cancel_order_by_external_oid(
        self, symbol: str, external_oid: str
    ) -> ApiResponse[None]:
        return await self._make_request(
            "POST",
            "/private/order/cancel_with_external",
            {"symbol": symbol, "externalOid": external_oid},
        )

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> ApiResponse[None]:
        params = {"symbol": symbol} if symbol else {}
        return await self._make_request("POST", "/private/order/cancel_all", params)

    # Trigger order management
    async def create_trigger_order(
        self, trigger_order_request: TriggerOrderRequest
    ) -> ApiResponse[int]:
        return await self._make_request(
            "POST", "/private/planorder/place", asdict(trigger_order_request, dict_factory=asdict_factory_with_enum_support)
        )

    async def cancel_trigger_orders(
        self, orders: List[Dict[str, str]]
    ) -> ApiResponse[None]:
        return await self._make_request("POST", "/private/planorder/cancel", orders)

    async def cancel_all_trigger_orders(
        self, symbol: Optional[str] = None
    ) -> ApiResponse[None]:
        params = {"symbol": symbol} if symbol else {}
        return await self._make_request("POST", "/private/planorder/cancel_all", params)

    # Stop limit order management
    async def cancel_stop_limit_order(self, stop_plan_order_id: int) -> ApiResponse[None]:
        return await self._make_request(
            "POST", "/private/stoporder/cancel", [{"stopPlanOrderId": stop_plan_order_id}]
        )

    async def cancel_all_stop_limit_orders(
        self, symbol: Optional[str] = None, position_id: Optional[int] = None
    ) -> ApiResponse[None]:
        params = {}
        if symbol:
            params["symbol"] = symbol
        if position_id:
            params["positionId"] = position_id
        return await self._make_request("POST", "/private/stoporder/cancel_all", params)

    async def change_stop_limit_trigger_price(
        self,
        order_id: int,
        stop_loss_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
    ) -> ApiResponse[None]:
        params = {
            "orderId": order_id,
            "stopLossPrice": stop_loss_price,
            "takeProfitPrice": take_profit_price,
        }
        return await self._make_request("POST", "/private/stoporder/change_price", params)

    async def update_stop_limit_trigger_plan_price(
        self,
        stop_plan_order_id: int,
        stop_loss_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
    ) -> ApiResponse[None]:
        params = {
            "stopPlanOrderId": stop_plan_order_id,
            "stopLossPrice": stop_loss_price,
            "takeProfitPrice": take_profit_price,
        }
        return await self._make_request(
            "POST", "/private/stoporder/change_plan_price", params
        )

    # Convenience methods
    async def create_market_order(
        self,
        symbol: str,
        side: OrderSide,
        vol: float,
        leverage: int,
        external_oid: Optional[str] = None,
    ) -> ApiResponse[OrderId]:
        order_request = CreateOrderRequest(
            symbol=symbol,
            side=side,
            leverage=leverage,
            externalOid=external_oid,
            vol=vol,
            openType=OpenType.Isolated,
            type=OrderType.MarketOrder,
        )
        return await self.create_order(order_request)

    async def create_stop_loss(
        self,
        symbol: str,
        side: OrderSide,
        vol: float,
        price: float,
        leverage: int = 20,
    ) -> ApiResponse[int]:
        trigger_type = (
            TriggerType.LessThanOrEqual
            if side == OrderSide.CloseLong
            else TriggerType.GreaterThanOrEqual
        )
        trigger_request = TriggerOrderRequest(
            symbol=symbol,
            side=side,
            vol=vol,
            leverage=leverage,
            openType=OpenType.Isolated,
            orderType=OrderType.MarketOrder,
            executeCycle=ExecuteCycle.UntilCanceled,
            trend=TriggerPriceType.LatestPrice,
            triggerPrice=price,
            triggerType=trigger_type,
        )
        return await self.create_trigger_order(trigger_request)