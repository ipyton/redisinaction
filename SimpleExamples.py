import redis
import time
conn = redis.Redis()

QUIT = True
LIMIT = 20

def add_to_cart(session, item, count):
    if count <= 0:
        conn.hrem('cart:', session, item)
    else:
        conn.hset('cart:'+session,item,count)

# 删除商品和会话 , commodities would be dropped if session is invalid,it makes no sense

def clean_full_sessions():
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
          time.sleep(1)
          continue
        end_index = min(size-LIMIT, 100)
        sessions = conn.zrange('recent:', 0, end_index-1)

        session_keys = []
        for sess in sessions:
            session_keys.append('viewed:'+sess)
            session_keys.append('cart:'+sess)

        conn.delete(*session_keys)
        conn.hdel('login:',*sessions)
        conn.zrem('recent:',*sessions)


# web cache

def cache_request(request, callback):
    if not can_cache(conn,request):
        return callback(request)
    page_key = 'cache:'+hash_request(request)
    content = conn.get(page_key)
    if not content:
        content = callback(request)
        conn.setex(page_key, content, 300)
    return content


# data row cache

def schedule_row_cache(row_id, delay):
    conn.zadd('delay:', row_id, delay)
    conn.zadd('schedule:',row_id,time.time())

def cache_rows():
    while not QUIT:
        next = conn.zrange('schedule:', 0, 0, withscores=True)
        now = time.time()
        if not next or next[0][1] > now:
            time.sleep(.5)
            continue
        row_id = next[0][0]

        delay = conn.zscore('delay:', row_id)
        if delay < 0:

        row = Invetory


# website analysis

def update_token(token, usr, item=None):



def rescale_viewed():


def can_cache(request):



def hash_request():
