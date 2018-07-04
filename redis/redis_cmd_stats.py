from redis import StrictRedis 
from sys import argv
from random import randrange, random
from hashlib import md5
from functools import wraps
from time import time
from argparse import ArgumentParser

DEFAULT_INSERT_COUNT = 1000

KEY_SEQ_RANGE = 100000000
DEFAULT_TIMESTAMP = 1400000000
VALUES_PER_KEY= 100

TIME_MS = 1000
TIME_US = 1000000


def set_hashval(r, k, vc):
    '''
    === hash mode=== 
    key: STR
    values: { audience_group_id(field): timestamp(value) }
    example:
      key: ebb6523878aa9907523700000000000000018553
      values: {'119305335': '1400000036', ... '100224834': '1400000071'}
    '''
    v = { '{}'.format(randrange(1,KEY_SEQ_RANGE) + KEY_SEQ_RANGE) : DEFAULT_TIMESTAMP + n for n in xrange(vc) }
    return save_to_redis(r.hmset, k, v)

def set_strval(r, k, vc):
    '''
    === string mode=== 
    key: STR
    values: [{ id: #####, timestamp: #####}, ...  }
    example
      key: ebb6523878aa9907523700000000000000018553
      values: "[{'timestamp': 1400000000, 'id': 180069208}, ... ,{'timestamp': 1400000001, 'id': 128858566}]"
    '''
    v = str([ dict({'id': randrange(1,KEY_SEQ_RANGE)+KEY_SEQ_RANGE, 'timestamp': DEFAULT_TIMESTAMP + n}) for n in xrange(vc) ])
    return save_to_redis(r.set, k, v)

def save_to_redis(f, k, v):
    start_time = time()
    f(k,v)
    return time() - start_time

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-s', '--seed', type=int, default=1
        , help='seed value for key sequence')
    parser.add_argument('-m', '--mode', required=True, type=str, default='hash', choices=['str', 'hash', 'hashlong']
        , help='select test mode')
    parser.add_argument('-l', '--vals', type=int, default=VALUES_PER_KEY
        , help='Number of group id per audience id')
    parser.add_argument('-c', '--counts', type=int, default=DEFAULT_INSERT_COUNT
        , help='Number of executions. It is also the number of keys')

    options = parser.parse_args()

    seed = options.seed
    insert_count = options.counts
    values_per_key = options.vals

    start = (insert_count * seed) + 1
    end   = (insert_count * seed) + insert_count + 1
    unit_of_time = TIME_MS
    

    r = StrictRedis(host='music-ddl-test001-dbteamtest-jp2p-prod.lineinfra.com', port=7000, db=0)
    r.flushall()
    init_memory = r.info('Memory')['used_memory']
    
    f_setval = set_hashval
   
    if options.mode == 'str':
        f_setval = set_strval
    elif options.mode == 'hashlong':
        r.config_set('hash-max-ziplist-entries', 10)
    else:
        r.config_set('hash-max-ziplist-entries', 1024)
   
    print f_setval.__doc__
    
    result = []
    for i in xrange(start, end):
        k = '{:.20}{:020}'.format(md5(str(random())).hexdigest(), i)
        result.append(f_setval(r, k, values_per_key) * unit_of_time)
   
    used_memory = r.info('Memory')['used_memory'] - init_memory
    mem_per_key = used_memory / insert_count
    resp_avg = sum(result) / float(len(result))
    resp_min = min(result)
    resp_max = max(result)
    resp_total = sum(result)

    print '''
    -----------------------
    *** result
    keys/values per key: {}/{}
    used_memory: {} kb
    mem per key: {} bytes
    avg: {:.3f} ms, min: {:.3f} ms, max: {:.3f} ms, total: {:.3f} ms
    '''.format(insert_count, values_per_key, used_memory, mem_per_key
    , resp_avg, resp_min, resp_max, resp_max)
