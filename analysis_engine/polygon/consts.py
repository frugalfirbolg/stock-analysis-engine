"""
**Polygon Environment Variables**

.. code-block:: python

    
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
        None
    )
    POLYGON_PROXIES = os.getenv(
        'POLYGON_PROXIES',
        None)
    DEFAULT_FETCH_DATASETS="daily,minute,quote,stats,
    peers,news,financials,earnings,dividends,company"

"""

import os


FETCH_DAILY = 700
FETCH_MINUTE = 701
FETCH_QUOTE = 702
FETCH_NEWS = 703
FETCH_FINANCIALS = 704
FETCH_DIVIDENDS = 705
FETCH_COMPANY = 706
FETCH_TICKERS = 707

DATAFEED_DAILY = 700
DATAFEED_MINUTE = 701
DATAFEED_QUOTE = 702
DATAFEED_NEWS = 703
DATAFEED_FINANCIALS = 704
DATAFEED_DIVIDENDS = 705
DATAFEED_COMPANY = 706
DATAFEED_TICKERS = 707

DATAFEED_SET = [
    DATAFEED_DAILY,
    DATAFEED_MINUTE,
    DATAFEED_QUOTE,
    DATAFEED_NEWS,
    DATAFEED_FINANCIALS,
    DATAFEED_DIVIDENDS,
    DATAFEED_COMPANY,
    DATAFEED_TICKERS
]

DEFAULT_FETCH_DATASETS = [
    FETCH_DAILY,
    FETCH_MINUTE,
    FETCH_QUOTE,
    FETCH_NEWS,
    FETCH_FINANCIALS,
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
    FETCH_DIVIDENDS,
]

POLYGON_DATE_FORMAT = '%Y-%m-%d'
POLYGON_TICK_FORMAT = '%Y-%m-%d %H:%M:%S'
POLYGON_FETCH_MINUTE_FORMAT = '%H:%M'

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
    None
)
POLYGON_PROXIES = os.getenv(
    'POLYGON_PROXIES',
    None)
POLYGON_DATE_FIELDS = [
    'calendarDate',
    'reportPeriod',
    'updated',
    'date',
    'exDate',
    'declaredDate',
    'paymentDate',
    'recordDate']
POLYGON_TIME_FIELDS = [
    'closeTime',
    'close.time',
    'delayedPriceTime',
    'extendedPriceTime',
    'latestTime',
    'openTime',
    'open.time'
    'processedTime',
    't',
    'time',
    'timestamp',
    'lastUpdated']
POLYGON_EPOCH_FIELDS = []
POLYGON_SEC_EPOCH_FIELDS = [
    'datetime'
]
POLYGON_SECOND_FIELDS = []

ENV_FETCH_DATASETS = os.getenv(
    'DEFAULT_FETCH_DATASETS_POLYGON',
    None)
if ENV_FETCH_DATASETS:
    SPLIT_FETCH_DATASETS_POLYGON = ENV_FETCH_DATASETS.split(',')
    DEFAULT_FETCH_DATASETS = []
    for d in SPLIT_FETCH_DATASETS_POLYGON:
        if d == 'minute':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_MINUTE)
        elif d == 'daily':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_DAILY)
        elif d == 'quote':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_QUOTE)
        elif d == 'news':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_NEWS)
        elif d == 'financials':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_FINANCIALS)
        elif d == 'dividends':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_DIVIDENDS)
        elif d == 'company':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_COMPANY)
        elif d == 'tickers':
            DEFAULT_FETCH_DATASETS.append(
                FETCH_TICKERS)
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
    elif ft_type == FETCH_NEWS or ft_str == 'news':
        return 'news'
    elif ft_type == FETCH_FINANCIALS or ft_str == 'financials':
        return 'financials'
    elif ft_type == FETCH_DIVIDENDS or ft_str == 'dividends':
        return 'dividends'
    elif ft_type == FETCH_COMPANY or ft_str == 'company':
        return 'company'
    elif ft_type == FETCH_TICKERS or ft_str == 'tickers':
        return 'tickers'
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
    elif df_type == DATAFEED_NEWS:
        return 'news'
    elif df_type == DATAFEED_FINANCIALS:
        return 'financials'
    elif df_type == DATAFEED_DIVIDENDS:
        return 'dividends'
    elif df_type == DATAFEED_COMPANY:
        return 'company'
    elif df_type == DATAFEED_TICKERS:
        return 'tickers'
    else:
        return f'unsupported df_type={df_type}'
# end of get_datafeed_str
