import time

import redis

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 666
conn = redis.Redis()

'''
1.time_sort
2.metadata
3.votes_sort
4.vote_list
'''


def article_vote(usr, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS  # 超过一周的不计入统计
    if conn.zscore('time', article) < cutoff:
        return False

    article_id = article.split(':')[-1]
    if conn.sadd('voted:' + article_id, usr):
        conn.hincrby(article, 'votes', 1)
        conn.zincrby('score:', article, VOTE_SCORE)
        conn.zadd()


def post_article(user, title, link):
    article_id = str(conn.incr('article:'))  # article 记录着数字

    vote_target = 'voted:' + article_id
    now = time.time()

    conn.expire(vote_target, ONE_WEEK_IN_SECONDS)
    conn.sadd(vote_target, user)

    # metadata
    conn.hmset('article:' + article_id,
              {
                  'title': title,
                  'time': now,
                  'poster': user,
                  'votes': 1,
                  'link': link,
              }
              )

    article = 'article'+article_id

    # vote
    conn.zadd('time:', article, now)
    conn.zadd('score:', article, now+VOTE_SCORE)
    return article_id


page_size = 20


def get_top_articles(page, order_by):
    start = page*page_size
    end = (page+1)*page_size-1

    articles = conn.zrevrange(order_by, start, end)
    articles_with_data = []
    for article in articles:
        metadata = conn.hgetall(article)
        articles_with_data.append(metadata)

    return articles_with_data


def set_group_article(article_id, group_to_remove=[], group_to_add=[]):
    article = 'article:' + article_id

    for group in group_to_add:
        conn.sadd('group'+group, article)

    for group in group_to_remove:
        conn.srem('group'+group, article)


def get_group_article(page, group, order='score:'):
    key = order+group

    if not conn.exists(key):
        conn.zinterstore(key, ['group:'+group, order], aggregate='max')
        conn.expire(key, 60)

    return get_top_articles(page, key)


def main():
    post_article("tom", 'title:good','www.sssss.com')






if __name__ == "__main__":
    main()





