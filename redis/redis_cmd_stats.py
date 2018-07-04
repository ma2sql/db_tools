#!/usr/bin/python

from __future__ import print_function

from datetime import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

from redis import StrictRedis, RedisError
from collections import namedtuple, defaultdict
from operator import itemgetter
from signal import signal, SIGINT
from functools import reduce

from argparse import ArgumentParser


IGNORE_CMDS = (
    'PSYNC', 'REPLCONF', 'COMMAND',
    'SLOWLOG', 'CLUSTER', 'INFO',
    'AUTH', 'PING', 'CONFIG',
    'MONITOR', 'CLIENT', 'SLAVEOF',
    'PUBLISH', 'SUBSCRIBE', 'UNSUBSCRIBE',
    'PSUBSCRIBE', 'DBSIZE', 'SELECT',
)

CMD_ETC = 'ETC'
CMD_TOTAL = 'TOTAL'


class LoopHandler(object):
    SIGNAL = False


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_cmd_stats_closure(host, port, password, cmd, ignore_cmd):
    r = StrictRedis(host, port, password=password)

    def extract_key_value(key_value):
        (k, v) = key_value
        return (k.replace('cmdstat_', '').upper(), v['calls'])

    def is_command(k):
        return k in cmd or (k not in ignore_cmd and cmd == [])

    def stats_summary(stats, key_value):
        (k, v) = key_value
        if is_command(k):
            stats[k] = v
        else:
            stats[CMD_ETC] = stats[CMD_ETC] + v

        stats[CMD_TOTAL] = stats[CMD_TOTAL] + v

        return stats

    def get_cmd_stats():
        stats = {}
        try:
            stats_raw = dict(map(extract_key_value, r.info('commandstats').items()))
            stats = reduce(stats_summary, stats_raw.items(), {CMD_ETC:0, CMD_TOTAL: 0})
        except RedisError as e:
            raise e
        except KeyboardInterrupt as e:
            pass

        return stats

    return get_cmd_stats


def get_cluster_master_nodes(host, port):
    nodes = []
    try:
        r = StrictRedis(host=host, port=port)
        nodes = [k.replace('@',':').split(':')[:2] for k, v in sorted(r.cluster('nodes').items(), key=itemgetter(0))
                 if v['flags'] in ('master', 'myself,master')]
    except RedisError as e:
        print('Redis Error: ', e)
    finally:
        del r

    return nodes


def signal_handler(signal, frame):
    LoopHandler.SIGNAL = True


def main(nodes, password, cmd, ignore_cmd):

    signal(SIGINT, signal_handler)

    # RedisConn = namedtuple('RedisConn', 'host port r')
    # redis_conns = []
    redis_stats_funcs = []

    try:
        for i, node in enumerate(nodes):
            (host, port) = node
            redis_stats_funcs.append(get_cmd_stats_closure(host, port, password, cmd, ignore_cmd))

        pre_stats = defaultdict(lambda: 0)

        while True:
            if LoopHandler.SIGNAL:
                break

            success = 0

            temp_stats = defaultdict(lambda: 0)
            stats = defaultdict(lambda: 0)

            executor = ThreadPoolExecutor(max_workers=8)
            threads = map(executor.submit, redis_stats_funcs)

            for t in as_completed(threads):
                if not t.result() == {}:
                    success = success + 1

                for k, v in t.result().items():
                    temp_stats[k] = temp_stats[k] + v

            for k, v in temp_stats.items():
                stats[k] = v - pre_stats[k] if pre_stats[k] != 0 else 0
                pre_stats[k] = v

            print_data = ['{0}={1}'.format(*v) for v in sorted(stats.items(), key=itemgetter(0))
                          if v[0] not in (CMD_ETC, CMD_TOTAL)]

            print_data.append('{0}={1}'.format(CMD_ETC, stats[CMD_ETC]))
            print_data.append('{0}={1}'.format(CMD_TOTAL, stats[CMD_TOTAL]))

            print_string = ', '.join(print_data)

            print('[{now}] ({success}) {values}'.format(now=now(), success=success, values=print_string))
            sleep(1)

    except KeyboardInterrupt as e:
        pass

    except RedisError as e:
        print('RedisError:', e)


if __name__ == '__main__':
    description = '''Description:
    This is tool that prints OPS for each redis command.
    The output is just a single line that is a summary of the servers specified in the host-port option.
    '''
    usage = '''
    redis_cmd_stats.py --host-port=<HOST>:<PORT> ... --host-port=<HOST>:<PORT> --command=GET,SET,DEL
    '''
    parser = ArgumentParser(description=description, usage=usage)

    parser.add_argument("--host-port", action='append', type=str, required=True,
                        help='''Specify the server you want to monitor in the format '<HOST>:<PORT>'.
                        To monitor multiple servers, this can be specified multiple times.'''
                        )

    parser.add_argument("--cluster", action='store_true', default=False,
                        help='''Options for redis cluster.
                                You can specify this option
                                if you want to monitor the entire master node of the redis cluster.
                                You can specify only one host option, because the connection
                                information for the entire master node is obtained
                                by the 'cluster nodes' command.
                                '''
                        )

    parser.add_argument("--commands", action='store', type=str, default='',
                        help='''Specify the command you want to monitor.
                                If you use this option, ignore_command is ignored.'''
                        )

    parser.add_argument("--ignore-commands", action='store', type=str, default=','.join(IGNORE_CMDS),
                        help='''Specify the command you want to monitor.
                                If you use this option, default ignore commands are ignored.
                                Default: {0}'''.format(','.join(IGNORE_CMDS))
                        )

    parser.add_argument("--password", action='store', type=str,
                        help='''Password to use when connecting to redis.'''
                        )

    options = parser.parse_args()

    nodes = [s.split(':') for s in list(options.host_port)]

    if options.cluster:
        (host, port) = nodes[0]
        nodes = get_cluster_master_nodes(host, port)

    cmd = [c.strip().upper() for c in options.commands.strip().split(',') if c != '']
    ignore_cmd = [c.strip().upper() for c in options.ignore_commands.strip().split(',') if c != '']

    if nodes != []:
        print('-----------------------------------')
        print('* Servers:')
        for node in nodes:
            print('    {0}:{1}'.format(*node))

        print()
        print('* Number of Servers: {0}'.format(len(nodes)))
        print('* Cluster : {0}'.format(options.cluster))
        print('* Command: {0}'.format(', '.join(cmd)))
        print('* Ignore Command: {0}'.format(', '.join(ignore_cmd)))
        print('-----------------------------------')

        main(nodes, options.password, cmd, ignore_cmd)
