"""
Common Fetch for any supported Get from Polygon using HTTP

Supported environment variables:

::

    # debug the fetch routines with:
    export DEBUG_POLYGON_DATA=1

"""

import os
import datetime
import copy
import analysis_engine.consts as ae_consts
import analysis_engine.polygon.consts as polygon_consts
import analysis_engine.build_result as build_result
import analysis_engine.api_requests as api_requests
import analysis_engine.polygon.fetch_data as polygon_fetch_data
import analysis_engine.work_tasks.publish_pricing_update as publisher
import spylunking.log.setup_logging as log_utils

log = log_utils.build_colorized_logger(name=__name__)


def get_data_from_polygon(
        work_dict):
    """get_data_from_polygon

    Get data from Polygon - this requires an account

    :param work_dict: request dictionary
    """
    label = 'get_data_from_polygon'

    log.debug(
        f'task - {label} - start '
        f'work_dict={work_dict}')

    rec = {
        'data': None,
        'updated': None
    }
    res = {
        'status': ae_consts.NOT_RUN,
        'err': None,
        'rec': rec
    }

    ticker = None
    field = None
    ft_type = None

    try:

        ticker = work_dict.get('ticker', ae_consts.TICKER)
        field = work_dict.get('field', 'daily')
        ft_type = work_dict.get('ft_type', None)
        ft_str = str(ft_type).lower()
        label = work_dict.get('label', label)
        orient = work_dict.get('orient', 'records')
        backfill_date = work_dict.get('backfill_date', None)
        verbose = work_dict.get('verbose', False)

        polygon_req = None
        if ft_type == polygon_consts.FETCH_DAILY or ft_str == 'daily':
            ft_type == polygon_consts.FETCH_DAILY
            polygon_req = api_requests.build_polygon_fetch_daily_request(
                label=label)
        elif ft_type == polygon_consts.FETCH_MINUTE or ft_str == 'minute':
            ft_type == polygon_consts.FETCH_MINUTE
            polygon_req = api_requests.build_polygon_fetch_minute_request(
                label=label)
        elif ft_type == polygon_consts.FETCH_QUOTE or ft_str == 'quote':
            ft_type == polygon_consts.FETCH_QUOTE
            polygon_req = api_requests.build_polygon_fetch_quote_request(
                label=label)
        elif ft_type == polygon_consts.FETCH_NEWS or ft_str == 'news':
            ft_type == polygon_consts.FETCH_NEWS
            polygon_req = api_requests.build_polygon_fetch_news_request(
                label=label)
        elif ft_type == polygon_consts.FETCH_FINANCIALS or ft_str == 'financials':
            ft_type == polygon_consts.FETCH_FINANCIALS
            polygon_req = api_requests.build_polygon_fetch_financials_request(
                label=label)
        elif ft_type == polygon_consts.FETCH_DIVIDENDS or ft_str == 'dividends':
            ft_type == polygon_consts.FETCH_DIVIDENDS
            polygon_req = api_requests.build_polygon_fetch_dividends_request(
                label=label)
        elif ft_type == polygon_consts.FETCH_COMPANY or ft_str == 'company':
            ft_type == polygon_consts.FETCH_COMPANY
            polygon_req = api_requests.build_polygon_fetch_company_request(
                label=label)
        else:
            log.error(
                f'{label} - unsupported ft_type={ft_type} '
                f'ft_str={ft_str} ticker={ticker}')
            raise NotImplementedError
        # if supported fetch request type

        polygon_req['ticker'] = ticker
        clone_keys = [
            'ticker',
            's3_address',
            's3_bucket',
            's3_key',
            'redis_address',
            'redis_db',
            'redis_password',
            'redis_key'
        ]

        for k in clone_keys:
            if k in polygon_req:
                polygon_req[k] = work_dict.get(k, f'{k}-missing-in-{label}')
        # end of cloning keys

        if not polygon_req:
            err = (
                f'{label} - ticker={ticker} '
                f'did not build an Polygon request '
                f'for work={work_dict}')
            log.error(err)
            res = build_result.build_result(
                status=ae_consts.ERR,
                err=err,
                rec=rec)
            return res
        else:
            log.debug(
                f'{label} - ticker={ticker} '
                f'field={field} '
                f'orient={orient} fetch')
        # if invalid polygon request

        df = None
        try:
            if 'from' in work_dict:
                polygon_req['from'] = datetime.datetime.strptime(
                    '%Y-%m-%d %H:%M:%S',
                    work_dict['from'])
            if backfill_date:
                polygon_req['backfill_date'] = backfill_date
                polygon_req['redis_key'] = (
                    f'{ticker}_{backfill_date}_{field}')
                polygon_req['s3_key'] = (
                    f'{ticker}_{backfill_date}_{field}')

            if os.getenv('SHOW_SUCCESS', '0') == '1':
                log.info(
                    f'fetching Polygon {field} req={polygon_req}')
            else:
                log.debug(
                    f'fetching Polygon {field} req={polygon_req}')

            df = polygon_fetch_data.fetch_data(
                work_dict=polygon_req,
                fetch_type=ft_type,
                verbose=verbose)
            rec['data'] = df.to_json(
                orient=orient,
                date_format='iso')
            rec['updated'] = datetime.datetime.utcnow().strftime(
                '%Y-%m-%d %H:%M:%S')
        except Exception as f:
            log.error(
                f'{label} - ticker={ticker} field={ft_type} '
                f'failed fetch_data '
                f'with ex={f}')
        # end of try/ex

        if ae_consts.ev('DEBUG_POLYGON_DATA', '0') == '1':
            log.debug(
                f'{label} ticker={ticker} '
                f'field={field} data={rec["data"]} to_json')
        else:
            log.debug(
                f'{label} ticker={ticker} field={field} to_json')
        # end of if/else found data

        upload_and_cache_req = copy.deepcopy(polygon_req)
        upload_and_cache_req['celery_disabled'] = True
        upload_and_cache_req['data'] = rec['data']
        if not upload_and_cache_req['data']:
            upload_and_cache_req['data'] = '{}'
        use_field = field
        if use_field == 'news':
            use_field = 'news1'
        if 'redis_key' in work_dict:
            rk = work_dict.get('redis_key', polygon_req['redis_key'])
            if backfill_date:
                rk = f'{ticker}_{backfill_date}'
            upload_and_cache_req['redis_key'] = (
                f'{rk}_{use_field}')
        if 's3_key' in work_dict:
            sk = work_dict.get('s3_key', polygon_req['s3_key'])
            if backfill_date:
                sk = f'{ticker}_{backfill_date}'
            upload_and_cache_req['s3_key'] = (
                f'{sk}_{use_field}')

        try:
            update_res = publisher.run_publish_pricing_update(
                work_dict=upload_and_cache_req)
            update_status = update_res.get(
                'status',
                ae_consts.NOT_SET)
            log.debug(
                f'{label} publish update '
                f'status={ae_consts.get_status(status=update_status)} '
                f'data={update_res}')
        except Exception:
            err = (
                f'{label} - failed to upload polygon '
                f'data={upload_and_cache_req} to '
                f'to s3_key={upload_and_cache_req["s3_key"]} '
                f'and redis_key={upload_and_cache_req["redis_key"]}')
            log.error(err)
        # end of try/ex to upload and cache

        if not rec['data']:
            log.debug(
                f'{label} - ticker={ticker} no Polygon data '
                f'field={field} to publish')
        # end of if/else

        res = build_result.build_result(
            status=ae_consts.SUCCESS,
            err=None,
            rec=rec)

    except Exception as e:
        res = build_result.build_result(
            status=ae_consts.ERR,
            err=(
                f'failed - get_data_from_polygon '
                f'dict={work_dict} with ex={e}'),
            rec=rec)
    # end of try/ex

    log.debug(
        f'task - get_data_from_polygon done - '
        f'{label} - '
        f'status={ae_consts.get_status(res["status"])} '
        f'err={res["err"]}')

    return res
# end of get_data_from_polygon
