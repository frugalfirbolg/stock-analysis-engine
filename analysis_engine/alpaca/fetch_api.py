"""
Fetch API calls for pulling data from
a valid Alpaca account, which in turn pulls from
Polygon and other sources.

.. warning:: Running these API calls will impact
    your account's monthly quota. Please be
    aware of your usage when calling these.

Please set the environment variable ``ALPACA_TOKEN`` to
your public account token before running these calls.

More steps can be found on the docs in the
`Alpaca API <https://stock-analysis-engine.readth
edocs.io/en/latest/alpaca_api.html#alpaca-api>`__

**Command Line Tool Fetching Examples**

With the Analysis Engine stack running you can use
the pip's included ``fetch`` command line tool with the
following arguments to pull data (and automate it).

**Fetch Minute Data**

::

    fetch -t AAPL -g min

**Fetch Daily Data**

::

    fetch -t AAPL -g day

**Fetch Quote Data**

::

    fetch -t AAPL -g quote

**Fetch Stats Data**

::

    fetch -t AAPL -g stats

**Fetch Peers Data**

::

    fetch -t AAPL -g peers

**Fetch News Data**

::

    fetch -t AAPL -g news

**Fetch Financials Data**

::

    fetch -t AAPL -g fin

**Fetch Earnings Data**

::

    fetch -t AAPL -g earn

**Fetch Dividends Data**

::

    fetch -t AAPL -g div

**Fetch Company Data**

::

    fetch -t AAPL -g comp

**Command Line Fetch Debugging**

Add the ``-d`` flag to the ``fetch`` command to enable
verbose logging. Here is an example:

::

    fetch -t AAPL -g news -d

"""

import pandas as pd
import analysis_engine.consts as ae_consts
import analysis_engine.utils as ae_utils
import analysis_engine.dataset_scrub_utils as dataset_utils
import analysis_engine.alpaca.consts as alpaca_consts
import analysis_engine.alpaca.helpers_for_alpaca_api as alpaca_helpers
import spylunking.log.setup_logging as log_utils

log = log_utils.build_colorized_logger(name=__name__)


def fetch_daily(
        ticker=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """fetch_daily

    Fetch the Alpaca daily data for a ticker and
    return it as a ``pandas.DataFrame``.
    
    https://alpaca.markets/docs/api-documentation/api-v2/market-data/bars/

    .. code-block:: python

        import analysis_engine.alpaca.fetch_api as alpaca_fetch

        minute_df = alpaca_fetch.fetch_daily(ticker='SPY')
        print(minute_df)

    :param ticker: string ticker to fetch
    :param work_dict: dictionary of args
        used by the automation
    :param scrub_mode: optional - string
        type of scrubbing handler to run
    :param verbose: optional - bool to log for debugging
    """
    label = None

    if work_dict:
        if not ticker:
            ticker = work_dict.get('ticker', None)
        label = work_dict.get('label', None)

    use_url = (
        f'/bars/1D?symbols={ticker}')

    if verbose:
        log.info(
            f'{label} - minute - url={use_url} '
            f'req={work_dict} ticker={ticker} ')

    resp_json = alpaca_helpers.get_from_alpaca(
        url=use_url,
        token=alpaca_consts.ALPACA_TOKEN,
        verbose=verbose)

    df = pd.DataFrame(resp_json[ticker])
    df.columns = ['close', 'high', 'low', 'open', 'datetime', 'volume']

    if verbose:
        log.info(
            f'{label} - minute - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if 'datetime' not in df:
        log.error(
            f'unable to download Alpaca minute '
            f'data for {ticker} '
            f'df: {df} from url: {use_url} with response: {resp_json}')
        return df

    if len(df.index) == 0:
        return df

    alpaca_helpers.convert_datetime_columns(
        df=df)
    
    # make sure dates are set as strings in the cache
    df['datetime'] = df['datetime'].dt.strftime(
        ae_consts.COMMON_TICK_DATE_FORMAT)
    df.set_index(
        [
            'datetime'
        ])
    return df
# end of fetch_daily


def fetch_minute(
        ticker=None,
        backfill_date=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """fetch_minute

    Fetch the Alpaca minute intraday data for a ticker and
    return it as a ``pandas.DataFrame``.
    
    https://alpaca.markets/docs/api-documentation/api-v2/market-data/bars/

    .. code-block:: python

        import analysis_engine.alpaca.fetch_api as alpaca_fetch

        minute_df = alpaca_fetch.fetch_minute(ticker='SPY')
        print(minute_df)

    :param ticker: string ticker to fetch
    :param backfill_date: optional - date string formatted
        ``YYYY-MM-DD`` for filling in missing minute data
    :param work_dict: dictionary of args
        used by the automation
    :param scrub_mode: optional - string
        type of scrubbing handler to run
    :param verbose: optional - bool to log for debugging
    """
    label = None
    use_date = backfill_date
    from_historical_date = None
    last_close_to_use = None
    dates = []

    if work_dict:
        label = work_dict.get('label', None)
        use_date = work_dict.get('use_date', None)
        if not ticker:
            ticker = work_dict.get('ticker', None)
        if not backfill_date:
            use_date = work_dict.get('backfill_date', None)
        if 'from_historical_date' in work_dict:
            from_historical_date = work_dict['from_historical_date']
        if 'last_close_to_use' in work_dict:
            last_close_to_use = work_dict['last_close_to_use']
        if from_historical_date and last_close_to_use:
            dates = ae_utils.get_days_between_dates(
                from_historical_date=work_dict['from_historical_date'],
                last_close_to_use=last_close_to_use)

    use_url = (
        f'bars/1Min?symbols={ticker}')

    if use_date:
        use_url = (
            f'bars/1Min?symbols={ticker}&start={use_date}T09:30:00-04:00')

    if verbose:
        log.info(
            f'{label} - minute - url={use_url} '
            f'req={work_dict} ticker={ticker} '
            f'fhdate={from_historical_date} '
            f'last_close={last_close_to_use} '
            f'dates={dates}')

    resp_json = alpaca_helpers.get_from_alpaca(
        url=use_url,
        token=alpaca_consts.ALPACA_TOKEN,
        verbose=verbose)

    df = pd.DataFrame(resp_json.get(ticker))
    df.columns = ['close', 'high', 'low', 'open', 'datetime', 'volume']

    if verbose:
        log.info(
            f'{label} - minute - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if 'datetime' not in df:
        log.error(
            f'unable to download Alpaca minute '
            f'data for {ticker} on backfill_date={use_date} '
            f'df: {df} from url: {use_url} with response: {resp_json}')
        return df

    if len(df.index) == 0:
        return df

    alpaca_helpers.convert_datetime_columns(
        df=df)
    
    # make sure dates are set as strings in the cache
    df['datetime'] = df['datetime'].dt.strftime(
        ae_consts.COMMON_TICK_DATE_FORMAT)
    df.set_index(
        [
            'datetime'
        ])
    return df
# end of fetch_minute


def fetch_quote(
        ticker=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """fetch_quote

    Fetch the Alpaca quote data for a ticker and
    return as a ``pandas.DataFrame``.

    https://alpaca.markets/docs/api-documentation/api-v2/market-data/last-quote/

    .. code-block:: python

        import analysis_engine.iex.fetch_api as iex_fetch

        quote_df = iex_fetch.fetch_quote(ticker='SPY')
        print(quote_df)

    :param ticker: string ticker to fetch
    :param work_dict: dictionary of args
        used by the automation
    :param scrub_mode: optional - string
        type of scrubbing handler to run
    :param verbose: optional - bool to log for debugging
    """
    label = None
    if work_dict:
        if not ticker:
            ticker = work_dict.get('ticker', None)
        label = work_dict.get('label', None)

    use_url = (
        f'last_quote/stocks/{ticker}')

    if verbose:
        log.info(
            f'{label} - quote - url={use_url} '
            f'req={work_dict} ticker={ticker}')

    resp_json = alpaca_helpers.get_from_alpaca(
        url=use_url,
        token=alpaca_consts.ALPACA_TOKEN,
        verbose=verbose)

    df = pd.DataFrame([resp_json])

    if verbose:
        log.info(
            f'{label} - quote - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if len(df.index) == 0:
        return df

    alpaca_helpers.convert_datetime_columns(
        df=df)

    cols_to_drop = []
    remove_these = None
    if len(cols_to_drop) > 0:
        for c in df:
            if c in cols_to_drop:
                if not remove_these:
                    remove_these = []
                remove_these.append(c)

    if remove_these:
        df = df.drop(columns=remove_these)

    return df
# end of fetch_quote


def fetch_news(
        ticker=None,
        num_news=5,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """fetch_news

    Fetch the Polygon news data for a ticker and
    return it as a ``pandas.DataFrame``.

    https://polygon.io/docs/#get_v1_meta_symbols__symbol__news_anchor

    .. code-block:: python

        import analysis_engine.alpaca.fetch_api as alpaca_fetch

        news_df = alpaca_fetch.fetch_news(ticker='SPY')
        print(news_df)

    :param ticker: string ticker to fetch
    :param num_news: optional - int number of news
        articles to fetch
        (default is ``5`` articles)
    :param work_dict: dictionary of args
        used by the automation
    :param scrub_mode: optional - string
        type of scrubbing handler to run
    :param verbose: optional - bool to log for debugging
    """
    label = None
    if work_dict:
        if not ticker:
            ticker = work_dict.get('ticker', None)
        label = work_dict.get('label', None)
        if not num_news:
            num_news = int(work_dict.get('num_news', 5))

    use_url = (
        f'v1/meta/symbols/{ticker}/news')

    if verbose:
        log.info(
            f'{label} - news - url={use_url} '
            f'req={work_dict} ticker={ticker}')

    resp_json = alpaca_helpers.get_from_polygon(
        url=use_url,
        token=alpaca_consts.POLYGON_TOKEN,
        verbose=verbose)

    df = pd.DataFrame(resp_json)
    
    if verbose:
        log.info(
            f'{label} - news - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if len(df.index) == 0:
        return df

    if 'timestamp' not in df:
        log.error(
            f'unable to download Polygon news '
            f'data for {ticker} '
            f'df: {df} from url: {use_url} with response: {resp_json}')
        return df

    cols_to_drop = []
    remove_these = None
    if len(cols_to_drop) > 0:
        for c in df:
            if c in cols_to_drop:
                if not remove_these:
                    remove_these = []
                remove_these.append(c)

    if remove_these:
        df = df.drop(columns=remove_these)

    return df
# end of fetch_news


def fetch_financials(
        ticker=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """fetch_financials

    Fetch the Polygon financial data for a ticker and
    return it as a ``pandas.DataFrame``.

    https://polygon.io/docs/#get_v2_reference_financials__symbol__anchor

    .. code-block:: python

        import analysis_engine.alpaca.fetch_api as alpaca_fetch

        fin_df = alpaca_fetch.fetch_financials(ticker='SPY')
        print(fin_df)

    :param ticker: string ticker to fetch
    :param work_dict: dictionary of args
        used by the automation
    :param scrub_mode: optional - string
        type of scrubbing handler to run
    :param verbose: optional - bool to log for debugging
    """
    label = None
    if work_dict:
        if not ticker:
            ticker = work_dict.get('ticker', None)
        label = work_dict.get('label', None)

    use_url = (
        f'v2/reference/financials/{ticker}')

    if verbose:
        log.info(
            f'{label} - fins - url={use_url} '
            f'req={work_dict} ticker={ticker}')

    resp_json = alpaca_helpers.get_from_polygon(
        url=use_url,
        token=alpaca_consts.POLYGON_TOKEN,
        verbose=verbose)

    df = pd.DataFrame(resp_json.get('results')).set_index('updated')

    if verbose:
        log.info(
            f'{label} - fins - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if len(df.index) == 0:
        return df

    alpaca_helpers.convert_datetime_columns(
        df=df)

    cols_to_drop = []
    remove_these = None
    if len(cols_to_drop) > 0:
        for c in df:
            if c in cols_to_drop:
                if not remove_these:
                    remove_these = []
                remove_these.append(c)

    if remove_these:
        df = df.drop(columns=remove_these)

    return df
# end of fetch_financials


def fetch_dividends(
        ticker=None,
        timeframe='3m',
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """fetch_dividends

    Fetch the Polygon dividends data for a ticker and
    return it as a ``pandas.DataFrame``.

    https://polygon.io/docs/#get_v2_reference_dividends__symbol__anchor

    .. code-block:: python

        import analysis_engine.iex.fetch_api as iex_fetch

        div_df = iex_fetch.fetch_dividends(ticker='SPY')
        print(div_df)

    :param ticker: string ticker to fetch
    :param timeframe: optional - string for setting
        dividend lookback period used for
        (default is ``3m`` for three months)
    :param work_dict: dictionary of args
        used by the automation
    :param scrub_mode: optional - string
        type of scrubbing handler to run
    :param verbose: optional - bool to log for debugging
    """
    label = None
    if work_dict:
        if not ticker:
            ticker = work_dict.get('ticker', None)
        label = work_dict.get('label', None)
        if not timeframe:
            timeframe = work_dict.get('timeframe', '3m')

    use_url = (
        f'v2/reference/dividends/{ticker}')

    if verbose:
        log.info(
            f'{label} - divs - url={use_url} '
            f'req={work_dict} ticker={ticker}')

    resp_json = alpaca_helpers.get_from_polygon(
        url=use_url,
        token=alpaca_consts.POLYGON_TOKEN,
        verbose=verbose)

    df = pd.DataFrame(resp_json.get('results'))

    if verbose:
        log.info(
            f'{label} - divs - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if len(df.index) == 0:
        return df

    alpaca_helpers.convert_datetime_columns(
        df=df)

    cols_to_drop = []
    remove_these = None
    if len(cols_to_drop) > 0:
        for c in df:
            if c in cols_to_drop:
                if not remove_these:
                    remove_these = []
                remove_these.append(c)

    if remove_these:
        df = df.drop(columns=remove_these)

    return df
# end of fetch_dividends


def fetch_company(
        ticker=None,
        work_dict=None,
        scrub_mode='NO_SORT',
        verbose=False):
    """fetch_company

    Fetch the Polygon company data for a ticker and
    return it as a ``pandas.DataFrame``.

    https://polygon.io/docs/#get_v1_meta_symbols__symbol__company_anchor

    .. code-block:: python

        import analysis_engine.alpaca.fetch_api as alpaca_fetch

        comp_df = alpaca_fetch.fetch_company(ticker='SPY')
        print(comp_df)

    :param ticker: string ticker to fetch
    :param work_dict: dictionary of args
        used by the automation
    :param scrub_mode: optional - string
        type of scrubbing handler to run
    :param verbose: optional - bool to log for debugging
    """
    label = None
    if work_dict:
        if not ticker:
            ticker = work_dict.get('ticker', None)
        label = work_dict.get('label', None)

    use_url = (
        f'v1/meta/symbols/{ticker}/company')

    if verbose:
        log.info(
            f'{label} - comp - url={use_url} '
            f'req={work_dict} ticker={ticker}')

    resp_json = alpaca_helpers.get_from_polygon(
        url=use_url,
        token=alpaca_consts.POLYGON_TOKEN,
        verbose=verbose)

    df = pd.DataFrame([resp_json])

    if verbose:
        log.info(
            f'{label} - comp - url={use_url} '
            f'ticker={ticker} response '
            f'df={df}')

    if len(df.index) == 0:
        return df

    alpaca_helpers.convert_datetime_columns(
        df=df)

    cols_to_drop = []
    remove_these = None
    if len(cols_to_drop) > 0:
        for c in df:
            if c in cols_to_drop:
                if not remove_these:
                    remove_these = []
                remove_these.append(c)

    if remove_these:
        df = df.drop(columns=remove_these)

    return df
# end of fetch_company
