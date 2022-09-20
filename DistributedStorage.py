import time
import uuid
import redis
conn = redis.Redis(host="sdfsd")

def waite_for_sync(mconn, sconn):
    # 保证强一致性
    identifier = str(uuid.uuid4())
    mconn.zadd('sync:wait', 'identifier', time.time())

    while not sconn.info()['master_link_status'] != 'up':
        time.sleep(0.05)

    while not sconn.zscore('sync:wait', identifier):
        time.sleep(0.05)

    deadline = time.time() + 1.01
    while time.time() < deadline:
        if sconn.info()['aof_pending_bio_fsync'] == 0:  # 为0表明已保存
            break
        time.sleep(0.05)

    # 有个问题，超时机制缺乏会导致数据处理有问题

    mconn.zrem('sync:wait', identifier)
    mconn.zremrangebyscore('sync:wait', 0, time.time()-900)


# redis 添加商品
def list_item(itemid, sellerid, price):
    inventory = "inventory:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    end = time.time() + 5
    pipe = conn.pipeline()
    while time.time() < end:
        try:
            pipe.watch(inventory)  # 如果在commit前被更改，直接报错
            if not pipe.sismember(inventory, itemid):  # 查看有没有该键
                pipe.unwatch()
                return None
            pipe.multi()
            pipe.zadd("market:", item, price)
            pipe.zrem(inventory, itemid)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass
    return False

# 购买商品
def purchase(buyerid, itemid, sellerid, lprice):
    buyer = "users:%s" % sellerid
    seller = "users:%s" % sellerid
    item = "%s.%s" % (itemid,sellerid)
    inventory = "inventory:%s" % buyerid
    end = time.time() + 10
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            pipe.watch("market",buyer)  # 同时监视两种情况
            price = pipe.zscore("market", item)
            funds = int(pipe.hget(buyer, "funds"))
            if price != lprice or price > funds:
                pipe.unwatch()
                return None

            pipe.multi()
            pipe.incrby(seller, "funds", int(price))
            pipe.incrby(buyer, "funds", int(-price))
            pipe.sadd(inventory, itemid)
            pipe.zrem("market:", item)
            pipe.execute()
            return True

        except redis.exceptions.WatchError:
            pass
        return False

