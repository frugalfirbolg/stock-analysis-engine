"""
Fetch API calls for pulling data from
a valid Polygon account, which in turn pulls from
Polygon and other sources.

Please set the environment variable ``POLYGON_TOKEN`` to
your public account token before running these calls.

More steps can be found on the docs in the
`Polygon API <https://stock-analysis-engine.readth
edocs.io/en/latest/polygon_api.html#polygon-api>`__

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

import datetime
from math import ceil
import pandas as pd
import analysis_engine.consts as ae_consts
import analysis_engine.utils as ae_utils
import analysis_engine.dataset_scrub_utils as dataset_utils
import analysis_engine.polygon.consts as polygon_consts
import analysis_engine.polygon.helpers_for_polygon_api as polygon_helpers
import spylunking.log.setup_logging as log_utils

log = log_utils.build_colorized_logger(name=__name__)

def fetch_tickers(
    locale='us',
    start_page=1,
    end_page=None,
    work_dict=None,
    verbose=False):
    """fetch_tickers

    Fetch the stock tickers supported by Polygon and return it as a list.
    This request can take a very long time.

    https://polygon.io/docs/#get_v2_reference_tickers_anchor

    .. code-block:: python

        import analysis_engine.polygon.fetch_api as polygon_fetch

        tickers = polygon_fetch.fetch_tickers()
        print(tickers)
    
    :param locale: optional - string to filter results, 'us' or 'g' for example
    :param start_page: optional - integer page to start on
    :param end_page: optional - integer page to end on
    :param work_dict: dictionary of args
        used by the automation
    :param verbose: optional - bool to log for debugging
    """
    label = None
    query_params = {
        'sort': 'ticker',
        'market': 'stocks',
        'active': 'true',
        'perpage': 50,
        'page': 1
    }
    if work_dict:
        label = work_dict.get('label', None)
        if work_dict.get('locale'):
            query_params = work_dict.get('locale')
        
        query_params['page'] = work_dict.get('start_page', start_page)
        if end_page is None:
            end_page = work_dict.get('end_page', None)
    
    if locale:
        query_params['locale'] = locale
    if start_page:
        query_params['page'] = start_page
    
    use_url = (
        f'/v2/reference/tickers')
    
    if verbose:
        log.info(
            f'{label} - tickers - url={use_url} '
            f'req={work_dict} locale={locale} ')
    
    first_page_json = polygon_helpers.get_from_polygon(
        url=use_url,
        query_params=query_params,
        token=polygon_consts.POLYGON_TOKEN,
        verbose=verbose)
    
    if 'count' not in first_page_json:
        log.error(
            f'unable to download Polygon tickers '
            f'on page {query_params.page} '
            f'from url: {use_url} with response: {resp_json}')
        return
    
    max_page = ceil(first_page_json.get('count') / first_page_json.get('perPage'))
    if end_page:
        max_page = min(end_page, max_page)

    tickers = first_page_json.get('tickers')
    
    for page in range(1, max_page):
        query_params['page'] = page
        resp_json = polygon_helpers.get_from_polygon(
            url=use_url,
            query_params=query_params,
            token=polygon_consts.POLYGON_TOKEN,
            verbose=verbose)
        if 'count' not in first_page_json:
            log.error(
                f'unable to download Polygon tickers '
                f'on page {query_params.page} '
                f'from url: {use_url} with response: {resp_json}')
            continue
        tickers += resp_json.get('tickers')


    df = pd.DataFrame(tickers)
    
    polygon_helpers.convert_datetime_columns(
        df=df)
    return df
# end of fetch_tickers


def fetch_daily(
        ticker=None,
        backfill_date=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """fetch_daily

    Fetch the Polygon daily data for a ticker and
    return it as a ``pandas.DataFrame``.
    
    https://polygon.io/docs/#get_v2_aggs_ticker__ticker__range__multiplier___timespan___from___to__anchor

    .. code-block:: python

        import analysis_engine.polygon.fetch_api as polygon_fetch

        daily_df = polygon_fetch.fetch_daily(ticker='SPY')
        print(daily_df)

    :param ticker: string ticker to fetch
    :param backfill_date: optional - date string formatted
        ``YYYY-MM-DD`` for filling in missing daily data
    :param work_dict: dictionary of args
        used by the automation
    :param scrub_mode: optional - string
        type of scrubbing handler to run
    :param verbose: optional - bool to log for debugging
    """
    label = None
    use_date = backfill_date
    from_historical_date = None

    if work_dict:
        label = work_dict.get('label', None)
        if not backfill_date:
            use_date = work_dict.get('use_date', None)
        if not ticker:
            ticker = work_dict.get('ticker', None)
        if not backfill_date:
            use_date = work_dict.get('backfill_date', None)
        if 'from_historical_date' in work_dict:
            from_historical_date = work_dict['from_historical_date']
    
    if use_date is None:
        use_date = datetime.datetime.today().strftime('%Y-%m-%d')

    if from_historical_date is None:
        from_historical_date = (datetime.datetime.strptime(use_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    use_url = (
        f'/v2/aggs/ticker/{ticker}/range/1/day/{from_historical_date}/{use_date}')
    
    if verbose:
        log.info(
            f'{label} - daily - url={use_url} '
            f'req={work_dict} ticker={ticker} '
            f'fhdate={from_historical_date} ')

    resp_json = polygon_helpers.get_from_polygon(
        url=use_url,
        token=polygon_consts.POLYGON_TOKEN,
        verbose=verbose)

    df = pd.DataFrame(resp_json['results'])
    
    if verbose:
        log.info(
            f'{label} - daily - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if 't' not in df:
        log.error(
            f'unable to download Polygon daily '
            f'data for {ticker} '
            f'df: {df} from url: {use_url} with response: {resp_json}')
        return df

    if len(df.index) == 0:
        return df
    
    df.rename(columns={'v':'volume', 'vw':'vwap', 'o':'open', 'c':'close', 'h':'high', 'l':'low', 'n':'count', 't':'time'}, inplace=True)
    polygon_helpers.convert_datetime_columns(
        df=df)
    
    # make sure dates are set as strings in the cache
    df.rename(columns={'time':'date'}, inplace=True)
    df['date'] = df['date'].dt.strftime(
        ae_consts.COMMON_DATE_FORMAT)
    return df
# end of fetch_daily


def fetch_minute(
        ticker=None,
        backfill_date=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """fetch_minute

    Fetch the Polygon minute intraday data for a ticker and
    return it as a ``pandas.DataFrame``.
    
    https://polygon.io/docs/#get_v2_aggs_ticker__ticker__range__multiplier___timespan___from___to__anchor

    .. code-block:: python

        import analysis_engine.polygon.fetch_api as polygon_fetch

        minute_df = polygon_fetch.fetch_minute(ticker='SPY')
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

    if work_dict:
        label = work_dict.get('label', None)
        use_date = work_dict.get('use_date', None)
        if not ticker:
            ticker = work_dict.get('ticker', None)
        if not backfill_date:
            use_date = work_dict.get('backfill_date', None)
        if 'from_historical_date' in work_dict:
            from_historical_date = work_dict['from_historical_date']
    
    if use_date is None:
        use_date = datetime.datetime.today().strftime('%Y-%m-%d')

    if from_historical_date is None:
        from_historical_date = use_date

    use_url = (
        f'/v2/aggs/ticker/{ticker}/range/1/minute/{from_historical_date}/{use_date}')

    if verbose:
        log.info(
            f'{label} - minute - url={use_url} '
            f'req={work_dict} ticker={ticker} '
            f'fhdate={from_historical_date} ')
    resp_json = polygon_helpers.get_from_polygon(
        url=use_url,
        token=polygon_consts.POLYGON_TOKEN,
        verbose=verbose)

    df = pd.DataFrame(resp_json.get('results'))

    if verbose:
        log.info(
            f'{label} - minute - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if 't' not in df:
        log.error(
            f'unable to download Polygon minute '
            f'data for {ticker} on backfill_date={use_date} '
            f'df: {df} from url: {use_url} with response: {resp_json}')
        return df

    if len(df.index) == 0:
        return df

    df.rename(columns={'o':'open', 'c':'close', 'h':'high', 'l':'low', 'v':'volume', 'vw':'vwap', 't':'time', 'n':'count'}, inplace=True)

    polygon_helpers.convert_datetime_columns(
        df=df)
    
    # make sure dates are set as strings in the cache
    df['time'] = df['time'].dt.strftime(
        ae_consts.COMMON_TICK_DATE_FORMAT)
    return df
# end of fetch_minute


def fetch_quote(
        ticker=None,
        work_dict=None,
        scrub_mode='sort-by-date',
        verbose=False):
    """fetch_quote

    Fetch the Polygon quote data for a ticker and
    return as a ``pandas.DataFrame``.

    https://polygon.io/docs/#get_v1_last_quote_stocks__symbol__anchor

    .. code-block:: python

        import analysis_engine.polygon.fetch_api as polygon_fetch

        quote_df = polygon_fetch.fetch_quote(ticker='SPY')
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
        f'/v1/last_quote/stocks/{ticker}')

    if verbose:
        log.info(
            f'{label} - quote - url={use_url} '
            f'req={work_dict} ticker={ticker}')

    resp_json = polygon_helpers.get_from_polygon(
        url=use_url,
        token=polygon_consts.POLYGON_TOKEN,
        verbose=verbose)
    
    df = pd.DataFrame([resp_json['last']])
    if verbose:
        log.info(
            f'{label} - quote - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')
    
    if 'timestamp' not in df:
        log.error(
            f'unable to download Polygon quote '
            f'data for {ticker} '
            f'df: {df} from url: {use_url} with response: {resp_json}')
        return df

    if len(df.index) == 0:
        return df
    
    polygon_helpers.convert_datetime_columns(
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

        import analysis_engine.polygon.fetch_api as polygon_fetch

        news_df = polygon_fetch.fetch_news(ticker='SPY')
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

    resp_json = polygon_helpers.get_from_polygon(
        url=use_url,
        token=polygon_consts.POLYGON_TOKEN,
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

        import analysis_engine.polygon.fetch_api as polygon_fetch

        fin_df = polygon_fetch.fetch_financials(ticker='SPY')
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

    resp_json = polygon_helpers.get_from_polygon(
        url=use_url,
        token=polygon_consts.POLYGON_TOKEN,
        verbose=verbose)

    df = pd.DataFrame(resp_json.get('results'))

    if verbose:
        log.info(
            f'{label} - fins - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if len(df.index) == 0:
        return df

    polygon_helpers.convert_datetime_columns(
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

    resp_json = polygon_helpers.get_from_polygon(
        url=use_url,
        token=polygon_consts.POLYGON_TOKEN,
        verbose=verbose)

    df = pd.DataFrame(resp_json.get('results'))

    if verbose:
        log.info(
            f'{label} - divs - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if len(df.index) == 0:
        return df

    polygon_helpers.convert_datetime_columns(
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

        import analysis_engine.polygon.fetch_api as polygon_fetch

        comp_df = polygon_fetch.fetch_company(ticker='SPY')
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

    resp_json = polygon_helpers.get_from_polygon(
        url=use_url,
        token=polygon_consts.POLYGON_TOKEN,
        verbose=verbose)

    df = pd.DataFrame([resp_json])

    if verbose:
        log.info(
            f'{label} - comp - url={use_url} '
            f'ticker={ticker} response '
            f'df={df}')

    if len(df.index) == 0:
        return df

    polygon_helpers.convert_datetime_columns(
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
