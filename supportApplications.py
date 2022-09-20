# log process
import contextlib
import csv
import json
import logging
import time
import datetime

import redis
conn = redis.Redis("8.8.8.8")

SEVERITY = {
    logging.DEBUG: 'debug',
    logging.INFO: 'info',
    logging.WARNING: 'warning',
    logging.ERROR: 'error',
    logging.CRITICAL: 'critical'
}

SEVERITY.update((name, name) for name in SEVERITY.values())


def log_recent(name, message, severity=logging.INFO, pipe=None):
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = 'recent:%s:%s' % (name, severity)
    message = time.asctime() + ' ' + message
    pipe = pipe or conn.pipeline()
    pipe.lpush(destination, message)
    pipe.ltrim(destination, 0, 99)
    pipe.execute()

def log_common( name, message, severity = logging.INFO, timeout = 5):
    severity = str(SEVERITY.get(severity)).lower()
    destination = 'common:%s:%s' % (name, severity)
    start_key = destination + ':start'

    pipe = conn.pipeline()
    end = time.time()+timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat()

            existing = pipe.get(start_key)
            pipe.multi()
            # 同类型的键如果在之前出现过，则把目前的key变为从前的，并且设置当前的key为最新的
            if existing and existing < hour_start:
                pipe.rename(destination, destination+':last')
                pipe.rename(start_key, destination+':pstart')
                pipe.set(start_key, hour_start)
            elif not existing:
                pipe.set(start_key, hour_start)
            pipe.zincrby(destination, hour_start)
            return
        except redis.exceptions.WatchError:
            continue


PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]


def update_counter(name, count=1, now=None):
    now = now or time.time()
    pipe = conn.pipeline()
    for prec in PRECISION:
        pnow = int(now/prec) * prec
        hash = '%s%s' % (prec, name)
        pipe.zadd('known:', hash, 0)
        pipe.incrby('count:' + hash, pnow, count)
    pipe.execute()

def get_counter(name, precision):


def clean_counters():
    pipe = conn.pipeline(True)



# 存储统计数据
def update_stats():

def get_stats(context, type):
    key = 'stats:%s:%s' % (context, type)
    data = dict(conn.zrange(key, 0 ,-1, withscores=True))
    data['average'] = data['sum'] / data['count']
    numerator = data['sumsq'] - data['sum']**2 / data['count']
    data['stddev'] = (numerator / (data['count'] - 1 or 1)) ** .5
    return data

@contextlib.contextmanager
def access_time(context):
    start = time.time()
    yield

    delta = time.time() - start
    stats = update_stats(conn, context, 'AccessTime', delta)
    average = stats[1]/stats[0]

    pipe = conn.pipeline(True) # 返回一个可以支持事务的客户端
    pipe.zadd('slowest:AccessTime', context, average)
    pipe.zremrangebyrank('slowest:AccessTime', 0, -101)
    pipe.execute()


# shows usage of context man ager
def process_view(callback):
    with access_time(conn):
        return callback

# find ip and its country
def ip_to_score(ip_address):
    score = 0
    for v in ip_address.split('.'):
        score = score * 256 + int(v, 10)
    return score

def import_ips_to_redis(file_name):
    csv_file = csv.reader(open(file_name, 'rb'))
    for count, row in enumerate(csv_file):
        start_ip = row[0] if row else ' '
        if 'i' in start_ip.lower():
            continue
        if '.' in start_ip:
            start_ip = int(start_ip, 10)
        elif start_ip.isdigit():
            start_ip = int(start_ip, 10)
        else : continue
        city_id = row[2] + '_' +str(count)
        conn.zadd('ip2cityid:', city_id, start_ip)


def import_cities_to_redis(filename):
    for row in csv.reader(open(filename, 'rb')):
        if len(row) < 4 or not row[0].isdigit():
            continue
        row = [i.decode('latin-1') for i in row]
        city_id = row[0]
        country = row[1]
        region = row[2]
        city = row[3]
        conn.hset('cityid2city:', city_id, json.dumps([city, region, country])) #json.dumps 转化为json格式字符串


def find_city_by_ip(ip_address):
    if isinstance(ip_address, str):
        ip_address = ip_to_score(ip_address)

    city_id = conn.zrevrangebyscore('ip2cityid:', ip_address, 0, start = 0, num = 1)
    if not city_id:
        return None

    city_id = city_id[0].partition('_')[0]

    return json.loads(conn.hget('cityid2city:', city_id))


# 配置中心

LAST_CHECKED = None
IS_UNDER_MAINTENANCE = False

def is_under_maintenance(conn):
    global LAST_CHECKED, IS_UNDER_MAINTENANCE

    if LAST_CHECKED < time.time() - 1:
        LAST_CHECKED = time.time()
        IS_UNDER_MAINTENANCE = bool()
