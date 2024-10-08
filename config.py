import configparser

def init():
    global configSettings 
    global configStrategies
    configSettings = configparser.ConfigParser()  # создаём объекта парсера
    configStrategies = configparser.ConfigParser()  # создаём объекта парсера

    configSettings.read("settings.ini")  # читаем конфиг
    configStrategies.read("Strategies.ini")  # читаем конфиг

    global time_delta 
    time_delta = 5

    global is_first_cycle
    is_first_cycle = True

    global is_last_operations
    is_last_operations = False

    global is_fill_tickers_list_fault
    is_fill_tickers_list_fault = False

    global account_id_master
    account_id_master = ""

    global account_id_slave
    account_id_slave = ""

    global logger_common 
    logger_common= ""

    global logger_strategy
    logger_strategy= ""

    global tickers_list
    tickers_list = []