import asyncio
import math
import random
from mexcpy.mexcTypes import CreateOrderRequest, OpenType, OrderSide, OrderType
from mexcpy.api import MexcFuturesAPI

# Add the token from GET_TOKEN.png here
token = "WEB...";

api = MexcFuturesAPI(token, testnet=True)

def round_to_tick(value: float, tick_size: float) -> float:
    if tick_size <= 0:
        raise ValueError("Tick size must be positive")
    
    precision = -int(round(math.log10(tick_size)))
    rounded = round(value / tick_size) * tick_size
    return round(rounded, precision)

async def sleep(ms: int = 500) -> None:
    await asyncio.sleep(ms / 1000)

async def market_open_with_sl(
    symbol: str,
    side: OrderSide,
    profit_perc: float,
    exit_perc: float,
    vol: float
) -> None:
    order = await api.create_market_order(symbol, side, vol, 20)

    if order.success:
        print("Order created successfully", order)
        order_response = await api.get_order_by_order_id(str(order.data.orderId))

        entry_price = order_response.data.price

        take_profit_price = round_to_tick(
            entry_price * (1 + profit_perc) if side == OrderSide.OpenLong 
            else entry_price * (1 - profit_perc),
            0.1
        )

        stop_loss_price = round_to_tick(
            entry_price * (1 - exit_perc) if side == OrderSide.OpenLong 
            else entry_price * (1 + exit_perc),
            0.1
        )
        print({"entryPrice": entry_price, "takeProfitPrice": take_profit_price, "stopLossPrice": stop_loss_price})

        close_side = OrderSide.CloseLong if side == OrderSide.OpenLong else OrderSide.CloseShort
        await api.create_order(CreateOrderRequest(
            symbol=symbol,
            side=close_side,
            vol=vol,
            leverage=20,
            price=take_profit_price,
            openType=OpenType.Isolated,
            type=OrderType.PriceLimited,
            
        ))
        await api.create_stop_loss(symbol, close_side, vol, stop_loss_price)
    else:
        print("123123", order)


# MADE BY VECFUL
async def main():
    await market_open_with_sl("BTC_USDT", OrderSide.OpenLong, 0.01, 0.0025, 25)

    await sleep(1000)

    await api.cancel_all_orders("BTC_USDT")
    
    await api.cancel_all_trigger_orders("BTC_USDT")
    
    await sleep(1000)

    await api.create_market_order("BTC_USDT", OrderSide.CloseLong, 25, 20)

    await sleep(1000)

    long_prices = [85000 + i * 100 for i in range(5)]
    orders_promises1 = [
        api.create_order(CreateOrderRequest(
            symbol="BTC_USDT",
            side=OrderSide.OpenLong,
            vol=1,
            price=price,
            leverage=20,
            type=OrderType.PostOnlyMaker,
            openType=OpenType.Isolated,
            stopLossPrice=80000,
      ))
        for price in long_prices
    ]
    await asyncio.gather(*orders_promises1)

    await sleep(3000)

    await api.cancel_all_orders("BTC_USDT")


if __name__ == "__main__": 
    asyncio.run(main())
    # MADE BY VECFUL