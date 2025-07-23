from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Dict, Any, Union


class PositionSide(Enum):
    Long = 1
    Short = 2

class OrderCategory(Enum):
    LimitOrder = 1
    SystemTakeover = 2
    CloseDelegate = 3
    ADLReduction = 4

class OrderType(Enum):
    PriceLimited = 1
    PostOnlyMaker = 2
    TransactOrCancelInstantly = 3
    TransactCompletelyOrCancel = 4
    MarketOrder = 5
    ConvertMarketToCurrentPrice = 6

class OrderState(Enum):
    Uninformed = 1
    Uncompleted = 2
    Completed = 3
    Cancelled = 4
    Invalid = 5

class ErrorCode(Enum):
    Normal = 0
    ParameterErrors = 1
    InsufficientBalance = 2
    PositionNotExist = 3
    PositionInsufficient = 4
    OrderPriceTooLowForLong = 5
    ClosePriceTooHighForLong = 6
    RiskQuotaExceeded = 7
    SystemCanceled = 8

class ExecuteCycle(Enum):
    Hours24 = 1
    Week = 2
    UntilCanceled = 3

class PositionType(Enum):
    Long = 1
    Short = 2

class OpenType(Enum):
    Isolated = 1
    Cross = 2

class PositionState(Enum):
    Holding = 1
    SystemAutoHolding = 2
    Closed = 3
    
class PositionMode(Enum):
    Hedge = 1
    OneWay = 2
    
class OrderSide(Enum):
    OpenLong = 1
    CloseShort = 2
    OpenShort = 3
    CloseLong = 4
    
class TriggerType(Enum):
    GreaterThanOrEqual = 1  # More than or equal
    LessThanOrEqual = 2  # Less than or equal

class TriggerOrderState(Enum):
    Untriggered = 1  # Untriggered
    Cancelled = 2  # Cancelled
    Executed = 3  # Executed
    Invalid = 4  # Invalid
    ExecutionFailed = 5  # Execution failed

class TriggerSide(Enum):
    Untriggered = 0  # Untriggered
    TakeProfit = 1  # Take profit
    StopLoss = 2  # Stop loss

class TriggerPriceType(Enum):
    LatestPrice = 1  # Latest price
    FairPrice = 2  # Fair price
    IndexPrice = 3  # Index price


@dataclass
class OrderId:
    """Order ID"""
    orderId: str
    ts: int

@dataclass
class CreateOrderRequest:
    """Contract name"""
    symbol: str
    """Volume of the order"""
    vol: float
    """Order direction"""
    side: OrderSide
    """Order type"""
    type: OrderType
    """Open type"""
    openType: OpenType
    """Price of the order. Not needed if its a market order"""
    price: Optional[float] = None
    """Leverage, required for Isolated Margin"""
    leverage: Optional[int] = None
    """Position ID, recommended for closing positions"""
    positionId: Optional[int] = None
    """External order ID"""
    externalOid: Optional[str] = None
    """Stop-loss price"""
    stopLossPrice: Optional[float] = None
    """Take-profit price"""
    takeProfitPrice: Optional[float] = None
    """Position mode"""
    positionMode: Optional['PositionMode'] = None
    """For one-way positions, if you need to only reduce positions, pass in true, and two-way positions will not accept this parameter."""
    reduceOnly: Optional[bool] = None

@dataclass
class TriggerOrderRequest:
    """Contract name"""
    symbol: str
    """Volume of the order"""
    vol: float
    """Order direction"""
    side: OrderSide
    """Open type"""
    openType: OpenType
    """Trigger price"""
    triggerPrice: float
    """Trigger type"""
    triggerType: TriggerType
    """Execution cycle"""
    executeCycle: ExecuteCycle
    """Order type"""
    orderType: OrderType
    """Trigger price type"""
    trend: TriggerPriceType
    """Position mode"""
    positionMode: Optional['PositionMode'] = None
    """Leverage, required for Isolated Margin"""
    leverage: Optional[int] = None
    """Executed price (not applicable for market orders)"""
    price: Optional[float] = None

@dataclass
class AssetInfo:
    """currency"""
    currency: str
    """position margin"""
    positionMargin: float  # position margin
    """frozen balance"""
    frozenBalance: float  # frozen balance
    """available balance"""
    availableBalance: float  # available balance
    """drawable balance"""
    cashBalance: float  # drawable balance
    """total equity"""
    equity: float  # total equity
    """unrealized profit and loss"""
    unrealized: float
    bonus: float
    availableCash: float
    """available margin"""
    availableOpen: float

@dataclass
class TransferRecord:
    id: int
    """flow number"""
    txid: str
    """Example: USDT"""
    currency: str
    amount: float
    """options: IN, OUT"""
    type: str
    """options: WAIT, SUCCESS, FAILED"""
    state: str
    """Example: 1631600000000"""
    createTime: int
    """Example: 1631600000000"""
    updateTime: int

@dataclass
class TransferRecords:
    pageSize: int
    totalCount: int
    totalPage: int
    currentPage: int
    resultList: List[TransferRecord]

@dataclass
class PositionInfo:
    positionId: int
    symbol: str
    positionType: PositionType
    openType: OpenType
    state: PositionState
    """holding volume"""
    holdVol: float  # holding volume
    """frozen volume"""
    frozenVol: float
    closeAvgPrice: float
    openAvgPrice: float
    liquidatePrice: float
    """original initial margin"""
    oim: float
    """initial margin, add or subtract items can be used to adjust the liquidate price"""
    im: float
    """holding fee"""
    holdFee: float
    """realized profit and loss"""
    realised: float
    leverage: int
    createTime: int
    updateTime: int
    """automatic margin"""
    autoAddIm: bool
    closeProfitLoss: float  # Not defined in the docs
    closeVol: float  # Not defined in the docs
    deductFeeList: List[Any]  # Not defined in the docs
    fee: float  # Not defined in the docs
    holdAvgPrice: float  # Not defined in the docs
    holdAvgPriceFullyScale: str  # Not defined in the docs
    newCloseAvgPrice: float  # Not defined in the docs
    newOpenAvgPrice: float  # Not defined in the docs
    openAvgPriceFullyScale: str  # Not defined in the docs
    positionShowStatus: Optional[str] = None  # Not defined in the docs. Returned only for historical positions
    profitRatio: Optional[float] = None  # Not defined in the docs
    version: Optional[int] = None  # Not defined in the docs
    marginRatio: Optional[float] = None  # Not defined in the docs. Returned only for open positions

@dataclass
class FundingRecord:
    id: int
    symbol: str
    positionType: PositionType
    positionValue: float
    funding: float
    rate: float
    settleTime: int  # timestamp

@dataclass
class FundingRecords:
    pageSize: int
    totalCount: int
    totalPage: int
    currentPage: int
    resultList: List[FundingRecord]



@dataclass
class Order:
    """Order ID"""
    orderId: str
    """Contract name"""
    symbol: str
    """Position ID"""
    positionId: int
    """Trigger price"""
    price: float
    """Trigger volume"""
    vol: float
    """Leverage"""
    leverage: int
    """Order direction"""
    side: OrderSide
    """Order category"""
    category: OrderCategory
    """Order type"""
    orderType: OrderType
    """Deal average price"""
    dealAvgPrice: float
    """Transaction volume"""
    dealVol: float
    """Order margin"""
    orderMargin: float
    """Used margin"""
    usedMargin: float
    """Taker fee"""
    takerFee: float
    """Maker fee"""
    makerFee: float
    """Close profit"""
    profit: float
    """Fee currency"""
    feeCurrency: str
    """Open type"""
    openType: OpenType
    """Order state"""
    state: OrderState
    """Error code"""
    errorCode: ErrorCode
    """Order creation time"""
    createTime: int
    """Order update time"""
    updateTime: int
    """External order ID"""
    externalOid: Optional[str] = None
    """Stop-loss price"""
    stopLossPrice: Optional[float] = None
    """Take-profit price"""
    takeProfitPrice: Optional[float] = None
    

    bboTypeNum: int = 0  # Not defined in the api
    dealAvgPriceStr: str = "0"  # Not defined in the api
    positionMode: int = 1  # Not defined in the api
    priceStr: str = "84385.6"  # Not defined in the api
    showCancelReason: int = 0  # Not defined in the api
    showProfitRateShare: int = 0  # Not defined in the api
    version: int = 1  # Not defined in the api



@dataclass
class TriggerOrder:
    """Trigger order ID"""
    id: str
    """Contract name"""
    symbol: str
    """Leverage"""
    leverage: int
    """Order direction"""
    side: OrderSide
    """Trigger price"""
    triggerPrice: float
    """Execute price"""
    price: float
    """Order volume"""
    vol: float
    """Open type"""
    openType: OpenType
    """Trigger type"""
    triggerType: TriggerType
    """Order state"""
    state: TriggerOrderState
    """Execution cycle (in hours)"""
    executeCycle: int
    """Trigger price type"""
    trend: TriggerPriceType
    """Error code on failed execution"""
    errorCode: ErrorCode
    """Order ID (returned on successful execution)"""
    """Order type"""
    orderType: OrderType
    """Creation time (timestamp in milliseconds)"""
    createTime: int
    """Update time (timestamp in milliseconds)"""
    updateTime: int
    
    positionMode: PositionMode  # Not defined in the docs
    orderId: Optional[int] = None
    lossTrend: TriggerPriceType = TriggerPriceType.LatestPrice  # Not defined in the docs
    priceProtect: int = 0  # Not defined in the docs
    profitTrend: TriggerPriceType = TriggerPriceType.LatestPrice  # Not defined in the docs
    reduceOnly: bool = False  # Not defined in the docs

@dataclass
class StopLimitOrder:
    """Stop-limit order ID"""
    id: int
    """Contract name"""
    symbol: str
    """Limit order ID (0 if based on a position)"""
    orderId: Union[int, str]
    """Position ID"""
    positionId: str
    """Stop-loss price"""
    stopLossPrice: float
    """Order state"""
    state: TriggerOrderState
    """Trigger direction"""
    triggerSide: TriggerSide
    """Position type"""
    positionType: PositionType
    """Trigger volume"""
    vol: float
    """Actual number of orders"""
    realityVol: float
    """Order ID after successful delegation"""
    placeOrderId: str
    """Error code (0 for normal)"""
    errorCode: ErrorCode
    """Whether the order status is terminal (0 = non-terminal, 1 = terminal)"""
    isFinished: int
    """Version number"""
    version: int
    """Creation time (timestamp in milliseconds)"""
    createTime: int
    """Update time (timestamp in milliseconds)"""
    updateTime: int
    """Take-profit price"""
    takeProfitPrice: Optional[float] = None

    closeTryTimes: Optional[int] = None  # Not defined in the docs
    lossTrend: Optional[TriggerPriceType] = None  # Not defined in the docs
    priceProtect: Optional[int] = None  # Not defined in the docs
    profitLossVolType: Optional[str] = None  # Not defined in the docs
    profitTrend: Optional[TriggerPriceType] = None  # Not defined in the docs
    profit_LOSS_VOL_TYPE_DIFFERENT: Optional[str] = None  # Not defined in the docs
    profit_LOSS_VOL_TYPE_SAME: Optional[str] = None  # Not defined in the docs
    reverseErrorCode: Optional[ErrorCode] = None  # Not defined in the docs
    reverseTryTimes: Optional[int] = None  # Not defined in the docs
    stopLossReverse: Optional[int] = None  # Not defined in the docs
    stopLossVol: Optional[float] = None  # Not defined in the docs
    takeProfitReverse: Optional[int] = None  # Not defined in the docs
    volType: Optional[int] = None  # Not defined in the docs



@dataclass
class Transaction:
    id: int
    symbol: str
    side: OrderSide
    vol: float
    price: float
    fee: float
    feeCurrency: str
    profit: float
    taker: bool  # Not defined in the docs
    category: OrderCategory
    orderId: int
    timestamp: int
    positionMode: PositionMode  # Not defined in the docs

@dataclass
class RiskLimitItem:
    """Contract name"""
    symbol: str
    """Position type"""
    positionType: PositionType
    """Current risk level"""
    level: int
    """Maximum position volume"""
    maxVol: float
    """Maximum leverage rate"""
    maxLeverage: int
    """Maintenance margin rate"""
    mmr: float
    """Initial margin rate"""
    imr: float

    leverage: int  # Not defined in the docs
    limitBySys: bool  # Not defined in the docs
    openType: OpenType  # Not defined in the docs

@dataclass
class RiskLimit:
    __root__: Dict[str, List[RiskLimitItem]]

@dataclass
class Leverage:
    positionType: PositionType
    level: int  # risk level
    imr: float  # The leverage risk limit level corresponds to initial margin rate
    mmr: float  # Leverage risk limit level corresponds to maintenance margin rate
    leverage: int  # leverage

    currentMmr: float  # Not defined in the docs
    limitBySys: bool  # Not defined in the docs
    maxVol: float  # Not defined in the docs
    openType: OpenType  # Not defined in the docs

@dataclass
class TradingFeeInfo:
    """Tiered trading fee rate"""
    level: int
    """Last 30 days' turnover"""
    dealAmount: float
    """Wallet balance from yesterday"""
    walletBalance: float
    """Maker fee"""
    makerFee: float
    """Taker fee"""
    takerFee: float
    """Maker fee discount"""
    makerFeeDiscount: float
    """Taker fee discount"""
    takerFeeDiscount: float

    feeType: int  # Not defined in the docs
    inviterKyc: str  # Not defined in the docs
    makerFeeDeduct: float  # Not defined in the docs
    mxDeduct: bool  # Not defined in the docs
    mxDiscount: bool  # Not defined in the docs
    takerFeeDeduct: float  # Not defined in the docs