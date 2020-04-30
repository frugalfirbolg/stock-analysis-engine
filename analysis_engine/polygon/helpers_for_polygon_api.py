
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse as urlparse
import pandas as pd
import requests
import analysis_engine.consts as ae_consts
import analysis_engine.polygon.consts as polygon_consts
import spylunking.log.setup_logging as log_utils

log = log_utils.build_colorized_logger(name=__name__)


def convert_datetime_columns(
        df,
        date_cols=None,
        second_cols=None,
        scols=None,
        tcols=None,
        ecols=None):
    """convert_datetime_columns

    Convert the Polygon date columns in the ``df`` to ``datetime`` objects

    :param df: ``pandas.DataFrame`` to set columns to
        datetime objects
    :param date_cols: list of columns to convert with a date string format
        formatted: ``YYYY-MM-DD``
    :param second_cols: list of columns to convert with a date string format
        formatted: ``YYYY-MM-DD HH:MM:SS``
    :param scols: list of columns to convert with a time format
        (this is for seconds epoch integers)
    :param tcols: list of columns to convert with a time format
        (this is for millisecond epoch integers)
    :param ecols: list of columns to convert with a time format
        (this is for nanosecond epoch integers)
    """
    date_cols = date_cols or polygon_consts.POLYGON_DATE_FIELDS
    second_cols = second_cols or polygon_consts.POLYGON_SECOND_FIELDS
    scols = scols or polygon_consts.POLYGON_SEC_EPOCH_FIELDS
    tcols = tcols or polygon_consts.POLYGON_TIME_FIELDS
    ecols = ecols or polygon_consts.POLYGON_EPOCH_FIELDS

    for col in date_cols:
        if col in df:
            df[col] = pd.to_datetime(
                df[col],
                format=polygon_consts.POLYGON_DATE_FORMAT,
                errors='coerce')

    for col in second_cols:
        if col in df:
            df[col] = pd.to_datetime(
                df[col],
                format=polygon_consts.POLYGON_TICK_FORMAT,
                errors='coerce')
    
    for scol in scols:
        if scol in df:
            df[scol] = pd.to_datetime(
                df[scol],
                unit='s',
                errors='coerce')

    for tcol in tcols:
        if tcol in df:
            df[tcol] = pd.to_datetime(
                df[tcol],
                unit='ms',
                errors='coerce')

    for ecol in ecols:
        if ecol in df:
            df[ecol] = pd.to_datetime(
                df[ecol],
                unit='ns',
                errors='coerce')
# end of convert_datetime_columns

def get_from_polygon(
    url,
    token=None,
    query_params=None,
    verbose=False):
    """get_from_polygon

    Get data from Polygon

    :param url: Polygon resource url
    :param query_params: optional - dictionary of query parameters
    :param token: optional - string token for your user's
        account
    :param verbose: optional - bool turn on logging
    """
    if token is None:
        token = polygon_consts.POLYGON_TOKEN
    url = (
        f'{polygon_consts.POLYGON_URL}{url}?apiKey={token}')
    for param_name, param_value in query_params:
        url += f'&{param_name}={param_value}'
    resp = requests.get(
        url,
        proxies=polygon_consts.POLYGON_PROXIES)
    if resp.status_code == requests.codes.OK:
        res_data = resp.json()
        if verbose:
            proxy_str = ''
            if polygon_consts.POLYGON_PROXIES:
                proxy_str = (
                    f'proxies={polygon_consts.POLYGON_PROXIES}')
            log.info(
                f'POLYGONAPI - response data for '
                f'url={url.replace(token, "REDACTED")} '
                f'{proxy_str}'
                f'status_code={resp.status_code} '
                f'data={ae_consts.ppj(res_data)}')
        return res_data
    raise Exception(
        f'Failed to get data from Polygon with '
        f'function=get_from_polygon '
        f'url={url} which sent '
        f'response {resp.status_code} - {resp.text}')
# end of get_from_polygon