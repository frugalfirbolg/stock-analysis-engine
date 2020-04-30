"""
Extract an Alpaca dataset from Redis and
return it as a ``pandas.DataFrame`` or None

Please refer to the `Extraction API reference
for additional support <https://stock-analysis-engine.readthedocs.io/
en/latest/extract.html>`__

"""

import copy
import analysis_engine.consts as ae_consts
import analysis_engine.utils as ae_utils
import analysis_engine.api_requests as api_requests
import analysis_engine.alpaca.consts as alpaca_consts
import analysis_engine.extract_utils as extract_utils
import spylunking.log.setup_logging as log_utils

log = log_utils.build_colorized_logger(name=__name__)
keys = {
    'company': alpaca_consts.DATAFEED_COMPANY,
    'daily': alpaca_consts.DATAFEED_DAILY,
    'dividends': alpaca_consts.DATAFEED_DIVIDENDS,
    'financials': alpaca_consts.DATAFEED_FINANCIALS,
    'minute': alpaca_consts.DATAFEED_MINUTE,
    'news': alpaca_consts.DATAFEED_NEWS,
    'quote': alpaca_consts.DATAFEED_QUOTE
}


def extract_daily_dataset(
        ticker=None,
        date=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """extract_daily_dataset

    Extract the Alpaca daily data for a ticker from Redis and
    return it as a tuple (status, ``pandas.Dataframe``)

    .. code-block:: python

        import analysis_engine.alpaca.extract_df_from_redis as alpaca_extract

        # extract by historical date is also supported as an arg
        # date='2019-02-15'
        daily_status, daily_df = alpaca_extract.extract_daily_dataset(
            ticker='SPY')
        print(daily_df)

    :param ticker: string ticker to extract
    :param date: optional - string date to extract
        formatted ``YYYY-MM-DD``
    :param work_dict: dictionary of args
    :param scrub_mode: type of scrubbing handler to run
    :param verbose: optional - boolean for turning on logging
    """
    return extract_dataset(
        key='daily',
        work_dict=work_dict,
        ticker=ticker,
        date=date,
        scrub_mode=scrub_mode,
        verbose=verbose)
# end of extract_daily_dataset


def extract_minute_dataset(
        ticker=None,
        date=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """extract_minute_dataset

    Extract the Alpaca minute intraday data for a ticker from Redis and
    return it as a tuple (status, ``pandas.Dataframe``)

    .. code-block:: python

        import analysis_engine.alpaca.extract_df_from_redis as alpaca_extract

        # extract by historical date is also supported as an arg
        # date='2019-02-15'
        minute_status, minute_df = alpaca_extract.extract_minute_dataset(
            ticker='SPY')
        print(minute_df)

    :param ticker: string ticker to extract
    :param date: optional - string date to extract
        formatted ``YYYY-MM-DD``
    :param scrub_mode: type of scrubbing handler to run
    :param work_dict: dictionary of args
    :param verbose: optional - boolean for turning on logging
    """
    return extract_dataset(
        key='minute',
        work_dict=work_dict,
        ticker=ticker,
        date=date,
        scrub_mode=scrub_mode,
        verbose=verbose)
# end of extract_minute_dataset


def extract_quote_dataset(
        ticker=None,
        date=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """extract_quote_dataset

    Extract the Alpaca quote data for a ticker from Redis and
    return it as a tuple (status, ``pandas.Dataframe``)

    .. code-block:: python

        import analysis_engine.alpaca.extract_df_from_redis as alpaca_extract

        # extract by historical date is also supported as an arg
        # date='2019-02-15'
        quote_status, quote_df = alpaca_extract.extract_quote_dataset(
            ticker='SPY')
        print(quote_df)

    :param ticker: string ticker to extract
    :param date: optional - string date to extract
        formatted ``YYYY-MM-DD``
    :param work_dict: dictionary of args
    :param scrub_mode: type of scrubbing handler to run
    :param verbose: optional - boolean for turning on logging
    """
    return extract_dataset(
        key='quote',
        work_dict=work_dict,
        ticker=ticker,
        date=date,
        scrub_mode=scrub_mode,
        verbose=verbose)
# end of extract_quote_dataset


def extract_news_dataset(
        ticker=None,
        date=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """extract_news_dataset

    Extract the Alpaca news data for a ticker from Redis and
    return it as a tuple (status, ``pandas.Dataframe``)

    .. code-block:: python

        import analysis_engine.alpaca.extract_df_from_redis as alpaca_extract

        # extract by historical date is also supported as an arg
        # date='2019-02-15'
        news_status, news_df = alpaca_extract.extract_news_dataset(
            ticker='SPY')
        print(news_df)

    :param ticker: string ticker to extract
    :param date: optional - string date to extract
        formatted ``YYYY-MM-DD``
    :param work_dict: dictionary of args
    :param scrub_mode: type of scrubbing handler to run
    :param verbose: optional - boolean for turning on logging
    """
    return extract_dataset(
        key='news1',
        work_dict=work_dict,
        ticker=ticker,
        date=date,
        scrub_mode=scrub_mode,
        verbose=verbose)
# end of extract_news_dataset


def extract_financials_dataset(
        ticker=None,
        date=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """extract_financials_dataset

    Extract the Alpaca financial data for a ticker from Redis and
    return it as a tuple (status, ``pandas.Dataframe``)

    .. code-block:: python

        import analysis_engine.alpaca.extract_df_from_redis as alpaca_extract

        # extract by historical date is also supported as an arg
        # date='2019-02-15'
        fin_status, fin_df = alpaca_extract.extract_financials_dataset(
            ticker='SPY')
        print(fin_df)

    :param ticker: string ticker to extract
    :param date: optional - string date to extract
        formatted ``YYYY-MM-DD``
    :param work_dict: dictionary of args
    :param scrub_mode: type of scrubbing handler to run
    :param verbose: optional - boolean for turning on logging
    """
    return extract_dataset(
        key='financials',
        work_dict=work_dict,
        ticker=ticker,
        date=date,
        scrub_mode=scrub_mode,
        verbose=verbose)
# end of extract_financials_dataset


def extract_dividends_dataset(
        ticker=None,
        date=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """extract_dividends_dataset

    Extract the Alpaca dividends data for a ticker from Redis and
    return it as a tuple (status, ``pandas.Dataframe``)

    .. code-block:: python

        import analysis_engine.alpaca.extract_df_from_redis as alpaca_extract

        # extract by historical date is also supported as an arg
        # date='2019-02-15'
        div_status, div_df = alpaca_extract.extract_dividends_dataset(
            ticker='SPY')
        print(div_df)

    :param ticker: string ticker to extract
    :param date: optional - string date to extract
        formatted ``YYYY-MM-DD``
    :param work_dict: dictionary of args
    :param scrub_mode: type of scrubbing handler to run
    :param verbose: optional - boolean for turning on logging
    """
    return extract_dataset(
        key='dividends',
        work_dict=work_dict,
        ticker=ticker,
        date=date,
        scrub_mode=scrub_mode,
        verbose=verbose)
# end of extract_dividends_dataset


def extract_company_dataset(
        ticker=None,
        date=None,
        work_dict=None,
        scrub_mode='NO_SORT',
        verbose=False):
    """extract_company_dataset

    Extract the Alpaca company data for a ticker from Redis and
    return it as a tuple (status, ``pandas.Dataframe``)

    .. code-block:: python

        import analysis_engine.alpaca.extract_df_from_redis as alpaca_extract

        # extract by historical date is also supported as an arg
        # date='2019-02-15'
        comp_status, comp_df = alpaca_extract.extract_company_dataset(
            ticker='SPY')
        print(comp_df)

    :param ticker: string ticker to extract
    :param date: optional - string date to extract
        formatted ``YYYY-MM-DD``
    :param work_dict: dictionary of args
    :param scrub_mode: type of scrubbing handler to run
    :param verbose: optional - boolean for turning on logging
    """
    return extract_dataset(
        key='company',
        work_dict=work_dict,
        ticker=ticker,
        date=date,
        scrub_mode=scrub_mode,
        verbose=verbose)
# end of extract_company_dataset


def extract_dataset(
        key,
        ticker=None,
        date=None,
        work_dict=None,
        scrub_mode='NO_SORT',
        verbose=False):
    """extract_dataset

    Extract the Alpaca key data for a ticker from Redis and
    return it as a tuple (status, ``pandas.Dataframe``)

    :param key: Alpaca dataset key
    :param ticker: string ticker to extract
    :param date: optional - string date to extract
        formatted ``YYYY-MM-DD``
    :param work_dict: dictionary of args
    :param scrub_mode: type of scrubbing handler to run
    :param verbose: optional - boolean for turning on logging
    """
    if not key or key not in keys:
        log.error(
            f'unsupported extract key={key} in keys={keys}')
        return None
    label = key
    df_type = keys[key]
    df_str = alpaca_consts.get_datafeed_str(df_type=df_type)
    latest_close_date = ae_utils.get_last_close_str()

    use_date = date
    if work_dict:
        if not ticker:
            ticker = work_dict.get('ticker', None)
    if not work_dict:
        work_dict = api_requests.get_ds_dict(
            ticker=ticker)

    req = copy.deepcopy(work_dict)

    if not use_date:
        use_date = latest_close_date

    redis_key = f'{ticker}_{use_date}_{key}'
    req['redis_key'] = redis_key
    req['s3_key'] = redis_key

    if verbose:
        log.info(
            f'{label} - {df_str} - '
            f'date={date} '
            f'redis_key={req["redis_key"]} '
            f's3_key={req["s3_key"]} '
            f'{ae_consts.ppj(req)}')

    return extract_utils.perform_extract(
        df_type=df_type,
        df_str=df_str,
        work_dict=req,
        scrub_mode=scrub_mode,
        verbose=verbose)
# end of extract_dataset
