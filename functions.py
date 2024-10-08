from telethon import TelegramClient, events
from tinkoff.invest import Client,AsyncClient, RequestError, OrderDirection, OrderState, OrderType, Quotation,InstrumentIdType,OrderExecutionReportStatus,PriceType,StopOrderDirection,StopOrderExpirationType,StopOrderType
from tinkoff.invest.services import SandboxService,InstrumentsService,MarketDataService,ReplaceOrderRequest
from tinkoff.invest.sandbox.client import SandboxClient
from tinkoff.invest.utils import decimal_to_quotation, quotation_to_decimal
from tinkoff.invest import MoneyValue
from decimal import Decimal
from threading import Thread
import threading
import config
import asyncio
import nest_asyncio

import os
import sys
#import speech_recognition as sr
import logging
from datetime import timezone,datetime
#from pydub import AudioSegment
global_pumper_name = ""

config.init()
nest_asyncio.apply()

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S",
    stream=sys.stdout)

def preBuyCheck(price1,price2,actionType):
    result_rate = 0
    result_actType = actionType
    config.logger_common.info("    Функция preBuyCheck")
    config.logger_common.info(f"    Изначальный actionType = {actionType},price1 = {price1}, price2 = {price2}")
    if actionType == "LONG":
        if float(price2)/float(price1) > 1.0004:
            result_rate = 1
            config.logger_common.info(f"    result_rate = 1, result_actType = {result_actType} ")
        elif float(price2)/float(price1) > 1:
            result_rate = 0.5
            config.logger_common.info(f"    result_rate = 0.5, result_actType = {result_actType}")
        elif float(price1)/float(price2) > 1.0004:
            result_rate = 0.5
            result_actType = 'SHORT'
            config.logger_common.info(f"    result_rate = 0.5, result_actType = {result_actType}")
        elif float(price1)/float(price2) > 1:
            result_rate = 0
            result_actType = 'UNDEFINED'
            config.logger_common.info(f"    result_rate = 0, result_actType = {result_actType}")
        else:
            result_rate = 0.25
            result_actType = 'LONG'
            config.logger_common.info(f"    result_rate = 0.25, result_actType = {result_actType}")
        return [result_rate,result_actType]
    if actionType == "SHORT":
        if float(price1)/float(price2) > 1.0004:
            result_rate = 1
            config.logger_common.info(f"    result_rate = 1, result_actType = {result_actType}")
        elif float(price1)/float(price2) > 1:
            result_rate = 0.5
            config.logger_common.info(f"    result_rate = 0.5, result_actType = {result_actType}")
        elif float(price2)/float(price1) > 1.0004:
            result_rate = 0.5
            result_actType = 'LONG'
            config.logger_common.info(f"    result_rate = 0.5, result_actType = {result_actType}")
        elif float(price2)/float(price1) > 1:
            result_rate = 0
            result_actType = 'UNDEFINED'
            config.logger_common.info(f"    result_rate = 0, result_actType = {result_actType}")
        else:
            result_rate = 0.25
            result_actType = 'SHORT'
            config.logger_common.info(f"    result_rate = 0.25, result_actType = {result_actType}")
        return [result_rate,result_actType]

async def getMarginAttributes(token,accountId):
    async with AsyncClient(token) as client:
        result  = await client.users.get_margin_attributes(account_id=accountId)
        config.logger_common.info("    Получили данные по марже")
        return result

async def getOrderBook(token,accountId,ticker):
    async with AsyncClient(config.TIToken) as client:
        figi = config.configTickers[ticker] ['figi']
        result = await client.market_data.get_order_book(instrument_id = figi, depth = 35)
        config.logger_common.info("    Получили лист асков")
        return result

def getInstumentBy_callback(token,accountId,figi):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(getInstumentBy(token,accountId,figi))
    loop.close()

async def getInstumentBy(token,accountId,figi):
    async with AsyncClient(token) as client:
        result = await client.instruments.get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, 
            id=figi
            )
        config.logger_common.info("    Получили инфо по инструменту")
        return result 

async def getTickersAmount(ticker,percentage,action_type,portfolio,instrument_info,price):
    config.logger_common.info(f"Начало процесса getTickersAmount {ticker}")
    result = 0

    min_price_increment = quotation_to_decimal(instrument_info.min_price_increment)
    lot = Decimal(instrument_info.lot)

    config.logger_common.info("    приступаем к вычислению")

    free_money_amount = abs(quotation_to_decimal(portfolio.total_amount_currencies))
    percentage_money_amount_need = (quotation_to_decimal(portfolio.total_amount_portfolio) * Decimal(percentage))/100
    if (action_type == 'LONG'):
        short_enabled_flag = instrument_info.short_enabled_flag
        percentage_money_amount_can= free_money_amount
        if (percentage_money_amount_need >percentage_money_amount_can):
            percentage_money_amount_need = percentage_money_amount_can
        
        result = int(percentage_money_amount_need/(price * lot))
        config.logger_common.info(f"   кол-во лотов {result} по цене {price}")
        config.logger_common.info("    окончание вычислений для LONG позиции")
    if (action_type == 'SHORT'):
        if (short_enabled_flag):
            percentage_money_amount_can= free_money_amount
            if (percentage_money_amount_need >percentage_money_amount_can):
                percentage_money_amount_need = percentage_money_amount_can
            
            result = int(percentage_money_amount_need/(price * lot))
            config.logger_common.info(f"   кол-во лотов {result} по цене {price}")
            config.logger_common.info("    окончание вычислений для SHORT позиции")
        else:
            result = 0
            price = 0
    
    config.logger_common.info("Окончание процесса getTickersAmount")
        
    return result

async def getTickersAmountMargin(ticker,percentage,action_type,marginAttrs,instrument_info,price):
    config.logger_common.info(f"Начало процесса getTickersAmount {ticker}")
    result = 0

    liquid_portfolio = quotation_to_decimal(marginAttrs.liquid_portfolio)
    starting_margin = quotation_to_decimal(marginAttrs.starting_margin)

    min_price_increment = quotation_to_decimal(instrument_info.min_price_increment)
    lot = Decimal(instrument_info.lot)
    dlong = quotation_to_decimal(instrument_info.dlong)
    dshort = quotation_to_decimal(instrument_info.dshort)
    short_enabled_flag = instrument_info.short_enabled_flag
    config.logger_common.info("    приступаем к вычислению")
    if (liquid_portfolio > starting_margin):
        free_money_amount = liquid_portfolio-starting_margin
        percentage_money_amount_need = (liquid_portfolio * Decimal(percentage))/100
        if (action_type == 'LONG'):
            if dlong > 0:
                percentage_money_amount_can= free_money_amount/dlong
            else:
                percentage_money_amount_can= free_money_amount
            if (percentage_money_amount_need >percentage_money_amount_can):
                percentage_money_amount_need = percentage_money_amount_can
            
            result = int(percentage_money_amount_need/(price * lot))
            config.logger_common.info(f"   кол-во лотов {result} по цене {price}")
            config.logger_common.info("    окончание вычислений для LONG позиции")
        if (action_type == 'SHORT'):
            if (short_enabled_flag):
                if( dshort > 0):
                    percentage_money_amount_can= free_money_amount/dshort
                else:
                    percentage_money_amount_can= free_money_amount
                if (percentage_money_amount_need >percentage_money_amount_can):
                    percentage_money_amount_need = percentage_money_amount_can
                
                result = int(percentage_money_amount_need/(price * lot))
                config.logger_common.info(f"   кол-во лотов {result} по цене {price}")
                config.logger_common.info("    окончание вычислений для SHORT позиции")
            else:
                result = 0
                price = 0
        
        config.logger_common.info("Окончание процесса getTickersAmount")
        
    return result

def create_log_file(filename,loggername,ext):
    logger = logging.getLogger(loggername)
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    #handler = logging.FileHandler(os.path.dirname(os.path.abspath(__file__))+'/Logs/'+filename+'_'+str(datetime.today().strftime('%d-%m-%Y')) + '.'+ext, encoding='utf-8')
    handler = logging.FileHandler('/TI Python Strategies Bot V2/Logs/'+filename+'_'+str(datetime.today().strftime('%d-%m-%Y')) + '.'+ext, encoding='utf-8')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def buy_order(strategy,ticker,figi,quantity,price,token,account_id):
    
    figiFound = "N"
    needBuy = "Y" 
    need_create_logger = 'Y'
    if (strategy in logging.root.manager.loggerDict):
        need_create_logger = 'N'
    if (need_create_logger == 'Y'):
        config.logger_strategy = create_log_file(strategy,strategy,'log')
    else:
        config.logger_strategy = logging.getLogger(strategy)
    config.logger_strategy.info("\n")
    config.logger_strategy.info (f"Начало процесса buy_order, кол-во лотов {quantity}")
    with Client(token) as client:
        try:  
                config.logger_common.info(f"BUY Order with strategy {strategy}, ticker {ticker}")
                print (f"BUY Order with strategy {strategy}, ticker {ticker}")
                response = client.orders.post_order(
                    figi=figi,
                    quantity=quantity,
                    price=decimal_to_quotation(price),
                    direction=OrderDirection.ORDER_DIRECTION_BUY,
                    account_id=account_id,
                    order_type = OrderType.ORDER_TYPE_MARKET,
                    order_id=str(datetime.now(timezone.utc))
                )
                order_id = response.order_id
                
                config.logger_strategy.info (f"BUY Strategy-{strategy}  Тикер-{ticker}    Figi-{figi}     Количество-{response.lots_executed}   Цена-{quotation_to_decimal(response.executed_order_price)}")
                print (f"BUY Strategy-{strategy}  Тикер-{ticker}    Figi-{figi}     Количество-{response.lots_executed}   Цена-{quotation_to_decimal(response.executed_order_price)}")
        except Exception as error:
            print(error)
            config.logger_strategy.error(error)
            #logger.error("Posting trade takeprofit order failed. Exception: %s", error)
    config.logger_strategy.info (f"Окончание процесса buy_order")

def sell_order(strategy,ticker,figi,quantity,price,token,account_id):
    
    figiFound = "N"
    needBuy = "Y" 
    need_create_logger = 'Y'
    if (strategy in logging.root.manager.loggerDict):
        need_create_logger = 'N'
    if (need_create_logger == 'Y'):
        config.logger_strategy = create_log_file(strategy,strategy,'log')
    else:
        config.logger_strategy = logging.getLogger(strategy)
    config.logger_strategy.info("\n")
    config.logger_strategy.info (f"Начало процесса sell_order, кол-во лотов {quantity}")
    with Client(token) as client:
        try:     
            config.logger_common.info(f"SELL Order with strategy {strategy}, ticker {ticker}")
            print(f"SELL Order with strategy {strategy}, ticker {ticker}")
            response = client.orders.post_order(
                figi=figi,
                quantity=quantity,
                price=decimal_to_quotation(price),
                direction=OrderDirection.ORDER_DIRECTION_SELL,
                account_id=account_id,
                order_type = OrderType.ORDER_TYPE_MARKET,
                order_id=str(datetime.now(timezone.utc))
            )
            order_id = response.order_id
            
            config.logger_strategy.info (f"SELL  Strategy-{strategy}  Тикер-{ticker}    Figi-{figi}     Количество-{response.lots_executed}   Цена-{quotation_to_decimal(response.executed_order_price)}")
            print(f"SELL  Strategy-{strategy}  Тикер-{ticker}    Figi-{figi}     Количество-{response.lots_executed}   Цена-{quotation_to_decimal(response.executed_order_price)}")
        except Exception as error:
            print(error)
            config.logger_strategy.error(error)
            #logger.error("Posting trade takeprofit order failed. Exception: %s", error)
    config.logger_strategy.info (f"Окончание процесса SELL_order")

def sell_order1(pumper_name,ticker,figi,quantity,action_type):
    
    #logger_pumper = create_log_file(pumper_name,pumper_name,'log')
    if (pumper_name in logging.root.manager.loggerDict):
        need_create_logger = 'N'
    if (need_create_logger == 'Y'):
        config.logger_pumper = create_log_file(pumper_name,pumper_name,'log')
    else:
        config.logger_pumper = logging.getLogger(pumper_name)
    config.logger_pumper.info("\n")
    config.logger_pumper.info (f"Начало процесса sell_order, кол-во лотов {quantity}")
    with Client(config.TIToken) as client:
        try:
            loop = asyncio.get_event_loop()
            getTickersAmountResult = loop.run_until_complete(getTickersAmount(ticker,10,'LONG',pumper_name))
            order_book = getTickersAmountResult[3]
            order_book_asks = order_book.asks
            order_book_bids = order_book.bids
            lots = Decimal(getTickersAmountResult[4].lot)
            #price = order_book_bids[9].price
            if (action_type == 'LONG'):
                price = order_book_asks[0].price
                config.logger_common.info(f"Sell LONG Order with pumper {pumper_name}, ticker {ticker}")
                response = client.orders.post_order(
                    figi=figi[0],
                    quantity=quantity,
                    price=price,
                    direction=OrderDirection.ORDER_DIRECTION_SELL,
                    account_id=config.account_id,
                    order_type = OrderType.ORDER_TYPE_LIMIT,
                    order_id=str(datetime.now(timezone.utc))
                )
                print(response)
                config.list_selling_orders.append([pumper_name,ticker,"LONG",quantity,price,response.order_id,figi])
                config.logger_pumper.info (f"SELL LONG  Пампер-{pumper_name}  Тикер-{ticker}    Figi-{figi}     Количество-{quantity}   Цена-{quotation_to_decimal(response.executed_order_price) * response.lots_executed *lots}")
            if (action_type == 'SHORT'):    
                price = order_book_bids[0].price
                config.logger_common.info(f"BUY SHORT Order with pumper {pumper_name}, ticker {ticker}")
                response = client.orders.post_order(
                    figi=figi[0],
                    quantity=quantity,
                    price=price,
                    direction=OrderDirection.ORDER_DIRECTION_BUY,
                    account_id=config.account_id,
                    order_type = OrderType.ORDER_TYPE_LIMIT,
                    order_id=str(datetime.now(timezone.utc))
                )
                print(response)
                config.list_selling_orders.append([pumper_name,ticker,"SHORT",quantity,price,response.order_id,figi])
                config.logger_pumper.info (f"BUY SHORT  Пампер-{pumper_name}  Тикер-{ticker}    Figi-{figi}     Количество-{quantity}   Цена-{quotation_to_decimal(response.executed_order_price) * response.lots_executed *lots}")
        except Exception as error:
            print(error)
            config.logger_pumper.error(error)
            #logger.error("Posting trade takeprofit order failed. Exception: %s", error)

def getAccId(token):
    with Client(token) as client:
        accounts = client.users.get_accounts()
        return client.users.get_accounts().accounts[0].id

def getTickersAmount_Callback(arg1,arg2,arg3):
    result = asyncio.run(getTickersAmount(arg1,arg2,arg3))
    return result

def check_moex(i):    
    # i as interval in seconds    
    threading.Timer(int(i), check_moex,[int(i)]).start()    
    # put your action here
    #print ("check_moex")
    #config.logger_pumper = logging.getLogger("MOEX_News")
    for order in config.list_moex_orders:
        
        need_create_logger = 'Y'
        pumper_name = config.configPumpers[str(order[0])]["Name"]
        if (pumper_name in logging.root.manager.loggerDict):
            need_create_logger = 'N'
        if (need_create_logger == 'Y'):
            config.logger_pumper = create_log_file(pumper_name,pumper_name,'log')
        else:
            config.logger_pumper = logging.getLogger(pumper_name)
        #config.logger_pumper.info("\n")
        #timings = order[5]
        #timings = list(map(int, order[5].split(",")))

        #[result,price,marginAttrs,order_book,instrument_info]
        '''
        with Client(config.TIToken) as client:
            instrument_info = client.instruments.get_instrument_by(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, 
                id=config.configTickers[order[1]] ['figi']
                ).instrument
        dlong = float(quotation_to_decimal(instrument_info.dlong))
        dshort = float(quotation_to_decimal(instrument_info.dshort))
        '''

        for i in range( len(order[5])):
            order[5][i] = order[5][i]-1
            
            if order[5][i] == 0:
                with Client(config.TIToken) as client:
                    loop = asyncio.get_event_loop()
                    config.logger_common.info(f"    Идем получать инфу по кол-ву тикеров для сигнала из канала {pumper_name}")
                    getTickersAmountResult = loop.run_until_complete(getTickersAmount(order[1],config.configPumpers[order[0]]["MultiOrderPercentage"],'LONG',pumper_name))
                    #getTickersAmountResult = asyncio.run()
                    order[3] = getTickersAmountResult[0]
                    
                    order_book = getTickersAmountResult[3]
                    order_book_asks = order_book.asks
                    order_book_bids = order_book.bids
                    price = quotation_to_decimal(order_book_asks[0].price)
                    instrument_info = getTickersAmountResult[4]
                    dlong = float(quotation_to_decimal(instrument_info.dlong))
                    dshort = float(quotation_to_decimal(instrument_info.dshort))
                    #order_book = client.market_data.get_order_book(instrument_id = order[6][0], depth = 15)
                    
                    PriceDiff = float(config.configPumpers[str(order[0])]["PriceDiff"])
                    if (dlong == 0.2):
                        PriceDiff = 1+(PriceDiff-1)/1.75
                    if (dlong > 0.2 and dlong < 0.33):
                        PriceDiff = 1+(PriceDiff-1)/1.5
                    if (dlong > 0.33 and dlong < 0.5):
                        PriceDiff = 1+(PriceDiff-1)/1.25
                    config.logger_pumper.info(f"ставка риска лонг {dlong}, коэффициент цены {PriceDiff}")

                    #if float(price)>float(order[4])*1.005:
                    config.logger_pumper.info (f"Отслеживаем тикер {order[1]}: предыдущая цена-{order[4]}  Текущая цена-{price} ")
                    if float(price)>float(order[4])*float(PriceDiff):
                        if order[3] >0:
                            config.logger_pumper.info (f"предыдущая цена-{order[4]} меньше чем Текущая цена-{price} ")
                            try:
                                order[4] = price                         
                                buy_order(order[0],order[1],order[6],order[3],order[4],'LONG')
                                

                            except Exception as error:
                                print(error)
                                config.logger_pumper.error(error)
                                #logger.error("Posting trade takeprofit order failed. Exception: %s", error)
                            #sell_order(order[0],order[1],order[6],order[3])
                            #config.list_moex_orders.remove(order)
                            #break
                    
                    elif float(price)<float(order[4])/float(PriceDiff):
                        if order[3] >0:
                            config.logger_pumper.info (f"предыдущая цена-{order[4]} больше чем Текущая цена-{price} ")
                            try:
                                order[4] = price
                                buy_order(order[0],order[1],order[6],order[3],order[4],'SHORT')
                                

                            except Exception as error:
                                print(error)
                                config.logger_pumper.error(error)
                                #logger.error("Posting trade takeprofit order failed. Exception: %s", error)
                        #sell_order(order[0],order[1],order[6],order[3])
                        #config.list_moex_orders.remove(order)
                        #break
                    
                
        
        for timing in order[5]:
            if timing <= 0:
                order[5].remove(timing)
        if not order[5]:
            config.list_moex_orders.remove(order)
        


def make_order(i):    
    # i as interval in seconds    
    threading.Timer(int(i), make_order,[int(i)]).start()    
    # put your action here
    with Client(config.TIToken) as client:
        portfolio = client.operations.get_portfolio(account_id = config.account_id)
    for order in config.list_orders:
        isOrderCancelled = "N"
        figiFound = "N"
        isOrder = "Y"
        
        #timings = order[5]
        #timings = list(map(int, order[5].split(",")))    
        for position in portfolio.positions:
            if position.figi == order[6][0]:
                figiFound = "Y"        
                if position.quantity_lots.units == 0:
                    #client.orders.cancel_order(account_id=config.account_id,order_id=order[5])
                    isOrderCancelled = "Y"
                    order[0] = "CANCELLED"
                    #config.list_orders.remove(order)
                elif abs(position.quantity_lots.units) < order[3]:
                    order[3] = abs(position.quantity_lots.units)
        if figiFound != "Y":
            isOrder = "N"
            order[5] = []
        for i in range( len(order[5])):
            if order[0] != "CANCELLED":
                order[5][i] = order[5][i]-1
                if order[5][i] == 0:
                    #order[5].remove(order[5][i])
                    with Client(config.TIToken) as client:
                        order_book = client.market_data.get_order_book(instrument_id = order[6][0], depth = 5)
                        order_book_asks = order_book.asks
                        order_book_bids = order_book.bids
                        price_long = quotation_to_decimal(order_book_asks[0].price)
                        price_short = quotation_to_decimal(order_book_bids[0].price)
                        #portfolio = client.operations.get_portfolio(account_id = config.account_id)
                        for position in portfolio.positions:
                            if position.figi == order[6][0]:
                                figiFound = "Y"
                                if position.quantity_lots.units == 0:
                                    #client.orders.cancel_order(account_id=config.account_id,order_id=order[5])
                                    isOrderCancelled = "Y"
                                    order[0] = "CANCELLED"
                                    #config.list_orders.remove(order)
                                elif abs(position.quantity_lots.units) < order[3]:
                                    order[3] = abs(position.quantity_lots.units)
                        if figiFound != "Y":
                            isOrder = "N"
                            order[5] = []
                        if order[0] != "CANCELLED":
                            if (order[2] == 'LONG'):
                                if price_long/order[4] > 1.0016:
                                    sell_order(order[0],order[1],order[6],order[3],'LONG')
                                    #config.list_orders.remove(order)
                                    isOrderCancelled = "Y"
                                    order[0] = "CANCELLED"
                                    #break
                            if (order[2] == 'SHORT'):
                                if order[4]/price_short > 1.0016:
                                    sell_order(order[0],order[1],order[6],order[3],'SHORT')
                                    #config.list_orders.remove(order)
                                    isOrderCancelled = "Y"
                                    order[0] = "CANCELLED"
                                    #break
        if order[0] == "CANCELLED":
            config.list_orders.remove(order)
        else:
            for timing in order[5]:
                if timing <= 0:
                    order[5].remove(timing)
            if not order[5]:
                if  isOrder == "Y":
                    sell_order(order[0],order[1],order[6],order[3],order[2])
                    config.list_orders.remove(order)
                else:
                    config.list_orders.remove(order)
        #order[5] = order[5]-1
        #print (order)
        #if order[5] == 0:
        #    sell_order(order[0],order[1],order[6],order[3])
        #    config.list_orders.remove(order)

def check_order(i):    
    # i as interval in seconds    
    threading.Timer(int(i), check_order,[int(i)]).start()    
    # put your action here
    #print ("check_order")
   
    with Client(config.TIToken) as client:
        try:
            for order in config.list_selling_orders:
                need_create_logger = 'Y'
                if (order[0] in logging.root.manager.loggerDict):
                    need_create_logger = 'N'
                if (need_create_logger == 'Y'):
                    config.logger_pumper = create_log_file(order[0],order[0],'log')
                else:
                    config.logger_pumper = logging.getLogger(order[0])
                order_state = client.orders.get_order_state(account_id=config.account_id,order_id=order[5])
                if order_state.execution_report_status ==  OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:
                    #print(f"Заявка {order[5]} исполнена")
                    if order[2] == 'LONG':
                        config.logger_pumper.info (f"SELL LONG  Пампер-{order[0]}  Тикер-{order[1]}    Figi-{order[6]}     Количество-{order[3]}   Цена-{quotation_to_decimal(order_state.executed_order_price)}")
                    else:
                        config.logger_pumper.info (f"BUY SHORT  Пампер-{order[0]}  Тикер-{order[1]}    Figi-{order[6]}     Количество-{order[3]}   Цена-{quotation_to_decimal(order_state.executed_order_price)}")
                    config.list_selling_orders.remove(order)
                elif (order_state.execution_report_status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED 
                and order_state.execution_report_status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED):
                    #заявка в активном состоянии
                    config.logger_pumper.info (f"Ордер активен")
                    active_lots = order_state.lots_requested - order_state.lots_executed
                    #print(f"Заявка {order[5]} активна")
                    order_book = client.market_data.get_order_book(instrument_id = order[6][0], depth = 1)
                    order_book_asks = order_book.asks
                    order_book_bids = order_book.bids
                    if order[2] == 'LONG':
                        price = order_book_asks[0].price
                    else:
                        price = order_book_bids[0].price
                    if order_state.lots_executed == 0:
                        config.logger_pumper.info (f"идем по ветке, где 0 лотов ордера выполнены")
                        request = ReplaceOrderRequest()
                        request.quantity=active_lots
                        request.price=price
                        request.price_type = PriceType.PRICE_TYPE_CURRENCY
                        request.account_id=config.account_id
                        request.order_id=order[5]
                        request.idempotency_key=str(datetime.now(timezone.utc))
                        if (active_lots>0 and order[4] != price):
                            config.logger_pumper.info (f"Кол-во активных лотов {active_lots}")
                            replace_order = client.orders.replace_order(request)
                            order[4] = price
                            order[5] = replace_order.order_id
                    else:
                        if (active_lots>0 and order[4] != price):
                            config.logger_pumper.info (f"идем по ветке, где {active_lots} лотов ордера еще не выполнены")
                            response = client.orders.cancel_order(account_id = config.account_id, order_id=order[5])
                            if order[2] == 'LONG':
                                response = client.orders.post_order(
                                    figi=order[6][0],
                                    quantity=active_lots,
                                    price=price,
                                    direction=OrderDirection.ORDER_DIRECTION_SELL,
                                    account_id=config.account_id,
                                    order_type = OrderType.ORDER_TYPE_LIMIT,
                                    order_id=str(datetime.now(timezone.utc))
                                )
                                config.logger_pumper.info (f"Продаем {active_lots} лотов ")
                            else:
                                response = client.orders.post_order(
                                    figi=order[6][0],
                                    quantity=active_lots,
                                    price=price,
                                    direction=OrderDirection.ORDER_DIRECTION_BUY,
                                    account_id=config.account_id,
                                    order_type = OrderType.ORDER_TYPE_LIMIT,
                                    order_id=str(datetime.now(timezone.utc))
                                )
                                config.logger_pumper.info (f"Покупаем {active_lots} лотов ")
                            order[4] = price
                            order[5] = response.order_id
                else:
                   print(f"какая то ошибка")
                   config.list_selling_orders.remove(order)
                   config.logger_pumper.info (f"Ошибка, удаляем ордер")
        except Exception as error:
            print(error)
            config.logger_pumper.error(error)