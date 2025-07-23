from mexc_python.mexcpy.api import MexcFuturesAPI as API
from mexc_python.mexcpy.mexcTypes import CreateOrderRequest, OrderSide, OrderType, OpenType
from dataclasses import asdict
import asyncio
import getpass

USER_TOKEN = "WEB029506898cc35436d28428665685e59061ae4b900a6f95e06ab25a0341f438f1"

async def main():
    token = USER_TOKEN
    if token == "YOUR_USERTOKEN_HERE":
        token = getpass.getpass('Enter your MEXC USER_TOKEN: ')
    mexc_api = API(token=token)
    price = 50000
    usdt_amount = 25
    qty = usdt_amount / price
    qty = max(round(qty / 0.01) * 0.01, 0.01)
    qty = round(qty, 3)
    order = CreateOrderRequest(
        symbol="BTC_USDT",
        vol=25,
        side=OrderSide.OpenLong,
        type=OrderType.PriceLimited,
        openType=OpenType.Cross,
        leverage=100,
        price=price
    )
    payload = asdict(order)
    for k in ['side', 'type', 'openType']:
        if payload.get(k) is not None and hasattr(payload[k], 'value'):
            payload[k] = payload[k].value
    payload = {k: v for k, v in payload.items() if v is not None}
    print(f"[ORDER PAYLOAD] {payload}")
    resp = await mexc_api.create_order(order)
    print(f"[ORDER RESPONSE] {resp}")

if __name__ == "__main__":
    asyncio.run(main())