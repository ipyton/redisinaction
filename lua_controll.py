import time

import redis
conn = redis.Redis("localhost:8080")

# 两种操作，一种是
def script_load(script):
    sha = [None]  # 缓存
    def call(keys=[],args=[],force_eval=False):  # keys 记录想读取的键和想写入的键
        if not force_eval:
            if not sha[0]:
                sha[0] = conn.execute_command("SCRIPT", "LOAD", script, parse="LOAD")
        try:
            return conn.execute_command()
        except redis.exceptions.ResponseError as msg:
            if not msg.args[0].startswith("NOSCRIPT"):
                raise
        return conn.execute_command("EVAL", script, len(keys), *(keys + args))  # 直接执行脚本
    return call

#使用lua脚本创建状态信息
def create_status(uid, message, **data):
    create_status_lua = script_load('''
    local login = redis.call('hget', KEYS[1], 'login')
    if not login then
        return False
    end
    local id = redis.call('incr', KEYS[2])
    local key = string.format('status:%s', id)
    redis.call('hmset', key, 'login', login, 'id', id, unpack(ARGV))
    redis.call('hincrby', KEYS[1], 'posts', 1)
    return id
    ''')

    args = ['message', message, 'posted', time.time(), 'uid', uid]
    for key, value in data.iteritems():
        args.append(key)
        args.append(value)

    return create_status_lua(conn, ['user:%s' % uid, 'status:id:'], args)

def acquire_lock_with_timeout():

def release_lock():

def acquire_semaphore():

def refresh_semaphore():

def auto_complete_on_prefix():

def purchase_item_with_lock():

def purchase_item():

def sharded_push_helper():

def sharded_lpush():

def sharded_rpush():

def sharded_lpop():

def sharded_rpop():


def sharded_bpop_helper():

def sharded_blpop():

def sharded_brpop():

