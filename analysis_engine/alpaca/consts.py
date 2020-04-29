"""
**Alpaca and Polygon Environment Variables**

Alpaca Token is publishable key ID.

Polygon Token can be Alpaca Token.

.. code-block:: python

    ALPACA_API_VERSION = os.getenv(
        'ALPACA_API_VERSION',
        'v1')
    ALPACA_URL_BASE = os.getenv(
        'ALPACA_URL',
        f'https://paper-api.alpaca.markets/{ALPACA_API_VERSION}')
    ALPACA_TOKEN = os.getenv(
        'ALPACA_TOKEN',
        None)
    ALPACA_PROXIES = os.getenv(
        'ALPACA_PROXIES',
        None)
    POLYGON_URL = os.getenv(
        'POLYGON_URL',
        'https://api.polygon.io/'
    )
    POLYGON_API_VERSION = os.getenv(
        'POLYGON_API_VERSION',
        'v1'
    )
    POLYGON_TOKEN = os.getenv(
        'POLYGON_TOKEN',
        ALPACA_TOKEN
    )
    POLYGON_PROXIES = os.getenv(
        'POLYGON_PROXIES',
        None)
    DEFAULT_FETCH_DATASETS="daily,minute,quote,stats,
    peers,news,financials,earnings,dividends,company"

"""

import os


FETCH_DAILY = 800
FETCH_MINUTE = 801
FETCH_QUOTE = 802
FETCH_STATS = 803
FETCH_PEERS = 804
FETCH_NEWS = 805
FETCH_FINANCIALS = 806
FETCH_EARNINGS = 807
FETCH_DIVIDENDS = 808
FETCH_COMPANY = 809

DATAFEED_DAILY = 900
DATAFEED_MINUTE = 901
DATAFEED_QUOTE = 902
DATAFEED_STATS = 903
DATAFEED_PEERS = 904
DATAFEED_NEWS = 905
DATAFEED_FINANCIALS = 906
DATAFEED_EARNINGS = 907
DATAFEED_DIVIDENDS = 908
DATAFEED_COMPANY = 909

DEFAULT_FETCH_DATASETS = [
    FETCH_DAILY,
    FETCH_MINUTE,
    FETCH_QUOTE,
    FETCH_STATS,
    FETCH_PEERS,
    FETCH_NEWS,
    FETCH_FINANCIALS,
    FETCH_EARNINGS,
    FETCH_DIVIDENDS,
    FETCH_COMPANY
]
TIMESENSITIVE_DATASETS = [
    FETCH_MINUTE,
    FETCH_QUOTE,
    FETCH_NEWS
]
FUNDAMENTAL_DATASETS = [
    FETCH_QUOTE,
    FETCH_FINANCIALS,
    FETCH_EARNINGS,
    FETCH_DIVIDENDS,
    FETCH_STATS
]

ALPACA_DATE_FORMAT = '%Y-%m-%d'
ALPACA_TICK_FORMAT = '%Y-%m-%d %H:%M:%S'
IEX_FETCH_MINUTE_FORMAT = '%H:%M'

# Alpaca Environment Variables
#ALPACA_API_VERSION = os.getenv(
#    'ALPACA_API_VERSION',
#    'v1')
ALPACA_URL_BASE = os.getenv(
    'ALPACA_URL',
    f'https://paper-api.alpaca.markets/')
#api.alpaca.markets for live
ALPACA_TOKEN = os.getenv(
    'ALPACA_TOKEN',
    None)
ALPACA_PROXIES = os.getenv(
    'ALPACA_PROXIES',
    None)
#polygon API features are split between versions
#POLYGON_API_VERSION = os.getenv(
#    'POLYGON_API_VERSION',
#    'v1'
#)
POLYGON_URL = os.getenv(
    'POLYGON_URL',
    'https://api.polygon.io/'
)
POLYGON_TOKEN = os.getenv(
    'POLYGON_TOKEN',
    ALPACA_TOKEN
)
POLYGON_PROXIES = os.getenv(
    'POLYGON_PROXIES',
    None)
ALPACA_DATE_FIELDS = [
    'calendarDate',
    'reportPeriod',
    'updated',
    'date',
    'exDate',
    'declaredDate',
    'paymentDate',
    'recordDate']
ALPACA_TIME_FIELDS = [
    'closeTime',
    'close.time',
    'delayedPriceTime',
    'extendedPriceTime',
    'iexLastUpdated',
    'latestTime',
    'openTime',
    'open.time'
    'processedTime',
    'time',
    'timestamp',
    'lastUpdated']
ALPACA_EPOCH_FIELDS = [
    't',
    'datetime'
]
ALPACA_SECOND_FIELDS = []

ENV_FETCH_DATASETS = os.getenv(
    'DEFAULT_FETCH_DATASETS_IEX',
    None)
if ENV_FETCH_DATASETS:
    SPLIT_FETCH_DATASETS_IEX = ENV_FETCH_DATASETS.split(',')
    DEFAULT_FETCH_DATASETS = []
    for d in SPLIT_FETCH_DATASETS_IEX:
        if d == 'minute':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_MINUTE)
        elif d == 'daily':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_DAILY)
        elif d == 'quote':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_QUOTE)
        elif d == 'stats':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_STATS)
        elif d == 'peers':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_PEERS)
        elif d == 'news':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_NEWS)
        elif d == 'financials':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_FINANCIALS)
        elif d == 'earnings':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_EARNINGS)
        elif d == 'dividends':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_DIVIDENDS)
        elif d == 'company':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_COMPANY)
# end of building env-datasets to get

FETCH_DATASETS = DEFAULT_FETCH_DATASETS


def get_ft_str(
        ft_type):
    """get_ft_str

    :param ft_type: enum fetch type value to return
                    as a string
    """
    ft_str = str(ft_type).lower()
    if ft_type == FETCH_DAILY or ft_str == 'daily':
        return 'daily'
    elif ft_type == FETCH_MINUTE or ft_str == 'minute':
        return 'minute'
    elif ft_type == FETCH_QUOTE or ft_str == 'quote':
        return 'quote'
    elif ft_type == FETCH_STATS or ft_str == 'stats':
        return 'stats'
    elif ft_type == FETCH_PEERS or ft_str == 'peers':
        return 'peers'
    elif ft_type == FETCH_NEWS or ft_str == 'news':
        return 'news'
    elif ft_type == FETCH_FINANCIALS or ft_str == 'financials':
        return 'financials'
    elif ft_type == FETCH_EARNINGS or ft_str == 'earnings':
        return 'earnings'
    elif ft_type == FETCH_DIVIDENDS or ft_str == 'dividends':
        return 'dividends'
    elif ft_type == FETCH_COMPANY or ft_str == 'company':
        return 'company'
    else:
        return f'unsupported ft_type={ft_type}'
# end of get_ft_str


def get_datafeed_str(
        df_type):
    """get_datafeed_str

    :param df_type: enum fetch type value to return
                    as a string
    """
    if df_type == DATAFEED_DAILY:
        return 'daily'
    elif df_type == DATAFEED_MINUTE:
        return 'minute'
    elif df_type == DATAFEED_QUOTE:
        return 'quote'
    elif df_type == DATAFEED_STATS:
        return 'stats'
    elif df_type == DATAFEED_PEERS:
        return 'peers'
    elif df_type == DATAFEED_NEWS:
        return 'news'
    elif df_type == DATAFEED_FINANCIALS:
        return 'financials'
    elif df_type == DATAFEED_EARNINGS:
        return 'earnings'
    elif df_type == DATAFEED_DIVIDENDS:
        return 'dividends'
    elif df_type == DATAFEED_COMPANY:
        return 'company'
    else:
        return f'unsupported df_type={df_type}'
# end of get_datafeed_str
