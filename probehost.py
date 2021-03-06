# -*- coding: utf-8 -*-
# Created by Samwell
import argparse
import re
import json
import time

from urllib.parse import urlparse

import aioredis
import asyncio
import aiohttp
from scrapy import Selector

from awsfrwk.awsfunc import aws_common_vpc
from awsfrwk.awsmgr import get_awsenv_info, cleanup_awsenv
from awsfrwk.distinct import _get_reids
from awsfrwk.asyhelper import async_call

"""
Awsfrwk library,it is a powerful library which wrap amazon-web-services. AWS Lambda is serverless microservice
which executes your code on thousands worker in same time.
This script show how to use awsfrwk, and how to use coroutine. In my code, it starts 500(_probe_host_max) http 
connections in a single worker, and run 100(_probe_host_concurrent) worker to run you code, so the total concurrent 
number is 50,000.
To use awsfrwk: 
1. use aws_common_vpc to decorate on your function(homepage) , the function become a concurrent function.
2. set AWS environment variables, see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration
3. call your function in local. it will be call in aws-lambda worker pool. 
"""

_queue_host_key = 'host_%s_%d'
_queue_success_key = 'success_%s_%d'
_queue_failed_key = 'failed_%s_%d'

_ua_headers = {
    'User-Agent': "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)"
}

_url_error_re = re.compile(r'/((?:5|4)[0-9][0-9])\.htm')
_url_body_min = 50

# nginx wrote all error page by hard code , searched at https://github.com/nginx/nginx
_nginx_error_page = re.compile(r'<center><h1>((?:5|4|3)[0-9][0-9]).*?</h1></center>')


async def _set_result(pool, task_name, task_id, host, err=None):
    """
    save result into redis
    """
    async with pool.get() as conn:
        if err is None:
            hkey = _queue_success_key % (task_name, task_id)
            # ok, add host to redis set
            await conn.execute('sadd', hkey, host)
        else:
            hkey = _queue_failed_key % (task_name, task_id)
            # failed, add host,err-info to redis hash map
            await conn.execute('hset', hkey, host, err)


async def _parse_result(host, resp):
    """
    parse response of homepage, if there have any error, return it , or return None
    """
    rurl = urlparse(str(resp.url))
    # check: do site return friendly error: url is http://www.test.com/504.htm
    m = _url_error_re.search(rurl.path)
    if m:
        return "SITE:%s" % m.group(1)
    # check: is site redirect to other site, maybe nginx config have problem or host was recycled by dns manufacturer
    if not rurl.netloc.lower().endswith(host.lower()):
        return "SITE:REDIRECT"
    # body check
    html = await resp.text()
    sel = Selector(text=html)
    emlist = sel.xpath('//body/*').extract()
    sbody = ''.join(emlist)
    # check: is site homepage blank
    if len(sbody) == 0:
        return "BODY:Blank"
    else:
        m = _nginx_error_page.search(sbody)
        if m:
            return "NGX:%s" % m.group(1)
        elif len(sbody) < _url_body_min:
            return "BODY:Min"
    return None


async def _req_homepage(host, url, session, redispool, task_name, task_id):
    """
    access homepage of host, and parse response, save the result
    """
    try:
        async with session.get(url, allow_redirects=True) as resp:
            errinfo = await _parse_result(host, resp)
    except Exception as e:
        errinfo = 'EXPT:%s' % type(e).__name__
    await _set_result(redispool, task_name, task_id, host, errinfo)
    return errinfo


async def _query_homepage(task_name, task_id, redis_task_url, hostlist, loop):
    """
    use coroutine to access homepage of host, and save all result into redis.
    return a list of error hosts.
    """
    pool = await aioredis.create_pool(redis_task_url, loop=loop)
    try:
        timeout = aiohttp.ClientTimeout(total=120, connect=10, sock_connect=10, sock_read=100)
        resolver = aiohttp.AsyncResolver(loop=loop)
        conn = aiohttp.TCPConnector(resolver=resolver, limit=0, loop=loop)
        async with aiohttp.ClientSession(connector=conn, timeout=timeout, headers=_ua_headers, raise_for_status=True,
                                         loop=loop) as session:
            fslist = []
            fhlist = {}
            for h in hostlist:
                url = ('http://www.%s' % h) if not h.lower().startswith('www.') else ('http://%s' % h)
                f = asyncio.ensure_future(_req_homepage(h, url, session, pool, task_name, task_id), loop=loop)
                fslist.append(f)
                fhlist[id(f)] = h
            # waiting all connection finish
            done, pending = await asyncio.wait(fslist, loop=loop)
            retrylist = []
            for f in done:
                h = fhlist[id(f)]
                try:
                    err = f.result()
                    if err:
                        retrylist.append(h)
                except Exception:
                    retrylist.append(h)
            return retrylist
    finally:
        pool.close()
        await pool.wait_closed()


_redis_url = 'redis://:b70c76a2d44b43247bf79617ad4931f0@10.0.0.100:6379/0'

_probe_host_concurrent = 100
_probe_host_max = 500

_dns_re = re.compile(r'([a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+)')
_homepage_max_retry = 2


@aws_common_vpc(concurrent=_probe_host_concurrent, memory=256)
def homepage(hostlist, retry_count, *, task_name, task_id=None, debug=False):
    """
    This is concurrent function, decorate by aws_common_vpc. Use async_call to call coroutine function, check return list,
    and retry error host.
    Attention: call homepage recursively, it actually run in another worker, except homepage was called with debug=True.
    """
    retrylit = async_call(_query_homepage, (task_name, task_id, _redis_url, hostlist,))
    if retrylit and retry_count > 0:
        homepage(retrylit, retry_count - 1, task_name=task_name, task_id=task_id, debug=debug)


def _start(hostlist, task_name, task_id):
    """
    save hostlist to redis , and start homepage function
    """
    # save to redis
    redconn = _get_reids(_redis_url)
    host_key = _queue_host_key % (task_name, task_id)
    start = 0
    while start < len(hostlist):
        end = start + 100
        tmps = hostlist[start:end]
        redconn.sadd(host_key, *tmps)
        start = end

    # call the concurrent function
    start = 0
    while start < len(hostlist):
        end = start + _probe_host_max
        tmps = hostlist[start:end]
        homepage(tmps, _homepage_max_retry, task_name=task_name, task_id=task_id, debug=False)
        start = end


def _waiting(task_name, task_id):
    """
    waiting homepage function finish, and print processing rate
    """
    redconn = _get_reids(_redis_url)

    host_key = _queue_host_key % (task_name, task_id)
    success_key = _queue_success_key % (task_name, task_id)
    failed_key = _queue_failed_key % (task_name, task_id)

    def _get_processing():
        host_total = redconn.scard(host_key)
        success_total = redconn.scard(success_key)
        failed_total = redconn.hlen(failed_key)
        rate = (success_total + failed_total) / host_total * 100 if host_total != 0 else 0.0
        if rate > 100:
            rate = 100
        return rate

    func_list = [(homepage, task_name, task_id)]
    check = 0
    while True:
        time.sleep(5)
        queuing, error = get_awsenv_info(func_list)
        if queuing == 0:
            check += 1
            if check >= 3:
                cleanup_awsenv(func_list)
                break
        else:
            check = 0
        print('processing ...%d%%' % _get_processing())


def _save(outfile, task_name, task_id):
    """
    save result to file.
    """
    redconn = _get_reids(_redis_url)
    host_key = _queue_host_key % (task_name, task_id)
    success_key = _queue_success_key % (task_name, task_id)
    failed_key = _queue_failed_key % (task_name, task_id)

    host_total = redconn.scard(host_key)
    success_total = redconn.scard(success_key)
    check_set = redconn.sdiff(host_key, success_key)
    failed_dict = redconn.hgetall(failed_key)

    result = {
        'total': host_total,
        'success': success_total,
        'failed': {},
        'lost': []
    }
    for h in failed_dict:
        if h not in check_set:
            continue
        err = failed_dict[h]
        check_set.discard(h)
        result['failed'][h.decode()] = err.decode()

    for h in check_set:
        result['lost'].append(h.decode())

    outfile.write(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    outfile.close()


def _clean(task_name, task_id):
    redconn = _get_reids(_redis_url)
    host_key = _queue_host_key % (task_name, task_id)
    success_key = _queue_success_key % (task_name, task_id)
    failed_key = _queue_failed_key % (task_name, task_id)
    redconn.delete(host_key)
    redconn.delete(success_key)
    redconn.delete(failed_key)


def run(infile, outfile):
    # make task info.
    task_name = 'probehost'
    task_id = int(time.time())

    try:
        print('--------------------')
        print('checking ...')
        hostset = set()
        for line in infile:
            h = line.strip()
            if h:
                if not _dns_re.match(h):
                    print('host format error：%s, ignored' % h)
                else:
                    hostset.add(h)
        if not hostset:
            raise RuntimeError('infile content is empty!')
        hostlist = [h for h in hostset]

        print('--------------------')
        print('staring ...')
        _start(hostlist, task_name, task_id)

        print('--------------------')
        print('waiting ...')
        _waiting(task_name, task_id)

        print('--------------------')
        print('saving ...')
        _save(outfile, task_name, task_id)

        print('finish!')
    except Exception as e:
        print('exception: ', str(e))
    finally:
        _clean(task_name, task_id)


if __name__ == '__main__':
    # check argument
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version='%(prog)s 1.0')
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default='hosts.txt',
                        help='input-file is a text file of hosts, one host per line. (default: %(default)s)')
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default='result.txt',
                        help='output-file is a text file of json result.(default: %(default)s)')

    args = parser.parse_args()
    run(args.infile, args.outfile)
