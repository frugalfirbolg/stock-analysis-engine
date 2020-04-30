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
    work_dict=None,
    verbose=False):
    """fetch_tickers

    Fetch the stock tickers supported by Polygon and return it as a list.

    https://polygon.io/docs/#get_v2_reference_tickers_anchor

    .. code-block:: python

        import analysis_engine.polygon.fetch_api as polygon_fetch

        tickers = polygon_fetch.fetch_tickers()
        print(tickers)
    
    :param locale: optional - string to filter results, 'us' or 'g' for example
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
    if locale:
        query_params.locale = locale
    
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
    
    max_page = ceil(first_page_json.get('count') / first_page_json.get('perPage'))

    tickers = first_page_json.get('tickers')

    for page in range(1, max_page):
        query_params.page = page
        resp_json = polygon_helpers.get_from_polygon(
            url=use_url,
            query_params=query_params,
            token=polygon_consts.POLYGON_TOKEN,
            verbose=verbose)
        tickers += resp_json.get('tickers')
#{"page":1,"perPage":2,"count":32218,"status":"OK","tickers":[{"ticker":"A","name":"Agilent Technologies Inc.","market":"STOCKS","locale":"US","currency":"USD","active":true,"primaryExch":"NYE","updated":"2020-04-30","codes":{"cik":"0001090872","figiuid":"EQ0087231700001000","scfigi":"BBG001SCTQY4","cfigi":"BBG000C2V3D6","figi":"BBG000C2V541"},"url":"https://api.polygon.io/v2/tickers/A"},{"ticker":"AA","name":"Alcoa Corp","market":"STOCKS","locale":"US","currency":"USD","active":true,"primaryExch":"NYE","updated":"2020-04-30","codes":{"cik":"0001675149","figiuid":"EQ0000000045469815","scfigi":"BBG00B3T3HF1","cfigi":"BBG00B3T3HD3","figi":"BBG00B3T3HK5"},"url":"https://api.polygon.io/v2/tickers/AA"}]}

    df = pd.DataFrame(tickers)
    # TODO Indexing?
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
#{"ticker":"AAPL","status":"OK","queryCount":22,"resultsCount":21,"adjusted":true,"results":[{"v":3.7039737e+07,"vw":155.5485,"o":154.89,"c":157.92,"h":158.85,"l":154.23,"t":1546405200000,"n":1},{"v":9.1312195e+07,"vw":143.5801,"o":143.98,"c":142.19,"h":145.72,"l":142,"t":1546491600000,"n":1},{"v":5.857107e+07,"vw":146.8194,"o":144.53,"c":148.26,"h":148.5499,"l":143.8,"t":1546578000000,"n":1},{"v":5.4777764e+07,"vw":147.369,"o":148.7,"c":147.93,"h":148.83,"l":145.9,"t":1546837200000,"n":1},{"v":4.1025314e+07,"vw":150.1529,"o":149.56,"c":150.75,"h":151.82,"l":148.52,"t":1546923600000,"n":1},{"v":4.5099081e+07,"vw":152.9374,"o":151.29,"c":153.31,"h":154.53,"l":149.63,"t":1547010000000,"n":1},{"v":3.578067e+07,"vw":152.7516,"o":152.5,"c":153.8,"h":153.97,"l":150.86,"t":1547096400000,"n":1},{"v":2.7020707e+07,"vw":152.3964,"o":152.88,"c":152.29,"h":153.7,"l":151.51,"t":1547182800000,"n":1},{"v":3.2439186e+07,"vw":150.0164,"o":150.85,"c":150,"h":151.27,"l":149.22,"t":1547442000000,"n":1},{"v":2.8710324e+07,"vw":152.3575,"o":150.27,"c":153.07,"h":153.39,"l":150.05,"t":1547528400000,"n":1},{"v":3.0569706e+07,"vw":154.9455,"o":153.08,"c":154.94,"h":155.88,"l":153,"t":1547614800000,"n":1},{"v":2.982116e+07,"vw":154.9908,"o":154.2,"c":155.86,"h":157.66,"l":153.26,"t":1547701200000,"n":1},{"v":3.3751023e+07,"vw":156.9342,"o":157.5,"c":156.82,"h":157.88,"l":155.9806,"t":1547787600000,"n":1},{"v":3.039397e+07,"vw":154.2426,"o":156.41,"c":153.3,"h":156.73,"l":152.62,"t":1548133200000,"n":1},{"v":2.313057e+07,"vw":153.4328,"o":154.15,"c":153.92,"h":155.14,"l":151.7,"t":1548219600000,"n":1},{"v":2.5441549e+07,"vw":152.6824,"o":154.11,"c":152.7,"h":154.48,"l":151.74,"t":1548306000000,"n":1},{"v":3.3408893e+07,"vw":156.8508,"o":155.48,"c":157.76,"h":158.13,"l":154.32,"t":1548392400000,"n":1},{"v":2.6192058e+07,"vw":155.3584,"o":155.79,"c":156.3,"h":156.33,"l":153.66,"t":1548651600000,"n":1},{"v":4.1587239e+07,"vw":156.7823,"o":156.25,"c":154.68,"h":158.13,"l":154.11,"t":1548738000000,"n":1},{"v":6.108428e+07,"vw":163.2403,"o":163.25,"c":165.25,"h":166.15,"l":160.23,"t":1548824400000,"n":1},{"v":4.0739649e+07,"vw":167.0848,"o":166.11,"c":166.44,"h":169,"l":164.56,"t":1548910800000,"n":1}]}
    df = pd.DataFrame(resp_json['results'])
    df.columns = ['volume', 'vwap', 'open', 'close', 'high', 'low', 'date', 'count']

    if verbose:
        log.info(
            f'{label} - daily - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if 'date' not in df:
        log.error(
            f'unable to download Polygon daily '
            f'data for {ticker} '
            f'df: {df} from url: {use_url} with response: {resp_json}')
        return df

    if len(df.index) == 0:
        return df

    polygon_helpers.convert_datetime_columns(
        df=df)
    
    # make sure dates are set as strings in the cache
    df['date'] = df['date'].dt.strftime(
        ae_consts.COMMON_TICK_DATE_FORMAT)
    return df.set_index(
        [
            'date'
        ])
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
#{"ticker":"AAPL","status":"OK","queryCount":5000,"resultsCount":10,"adjusted":true,"results":[{"v":1.8338786e+07,"vw":156.34024,"o":154.4,"c":158.32,"h":158.43,"l":153.01,"t":1546385460000,"n":128044},{"v":1.7337595e+07,"vw":153.97233,"o":158.33,"c":145.07,"h":158.85,"l":143.13,"t":1546452120000,"n":125664},{"v":8.6630977e+07,"vw":143.63398,"o":145.15,"c":142.08,"h":146,"l":142,"t":1546518780000,"n":657671},{"v":5.4927951e+07,"vw":146.72439,"o":143.8,"c":148.4,"h":148.5499,"l":143.6,"t":1546585440000,"n":374426},{"v":2278,"vw":148.74325,"o":148.65,"c":148.77,"h":148.9,"l":148.55,"t":1546785420000,"n":30},{"v":5.2375189e+07,"vw":147.34313,"o":148.77,"c":148.06,"h":148.85,"l":145.9,"t":1546852080000,"n":347632},{"v":3.8733743e+07,"vw":150.11709,"o":148.5,"c":150.7,"h":151.82,"l":147.99,"t":1546918740000,"n":270715},{"v":2.1400421e+07,"vw":151.99832,"o":150.68,"c":153.0601,"h":153.3,"l":149.63,"t":1546985400000,"n":149252},{"v":2.2438331e+07,"vw":153.76471,"o":153.06,"c":152.15,"h":154.53,"l":151.6,"t":1547052060000,"n":154403},{"v":1.4794865e+07,"vw":152.03102,"o":152.15,"c":152.7,"h":152.83,"l":150.86,"t":1547118720000,"n":94878}]}

    df = pd.DataFrame(resp_json.get(ticker))
    df.columns = ['volume', 'vwap', 'open', 'close', 'high', 'low', 'time', 'count']

    if verbose:
        log.info(
            f'{label} - minute - url={use_url} '
            f'ticker={ticker} response '
            f'df={df.tail(5)}')

    if 'time' not in df:
        log.error(
            f'unable to download Polygon minute '
            f'data for {ticker} on backfill_date={use_date} '
            f'df: {df} from url: {use_url} with response: {resp_json}')
        return df

    if len(df.index) == 0:
        return df

    polygon_helpers.convert_datetime_columns(
        df=df)
    
    # make sure dates are set as strings in the cache
    df['time'] = df['time'].dt.strftime(
        ae_consts.COMMON_TICK_DATE_FORMAT)
    return df.set_index(
        [
            'time'
        ])
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
# {"last":{"askexchange":12,"askprice":290.99,"asksize":3,"bidexchange":12,"bidprice":290.97,"bidsize":1,"timestamp":1588261335537},"status":"success","symbol":"AAPL"}
    df = pd.DataFrame([resp_json])
# TODO format df
    if verbose:
        log.info(
            f'{label} - quote - url={use_url} '
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

    df = pd.DataFrame(resp_json.get('results')).set_index('updated')

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
