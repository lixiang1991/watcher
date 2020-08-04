#! /usr/bin/python
# -*- coding: utf-8 -*-

"""
@author: 黄炎、李翔
@time: 2018-7-25
文件名: database_manager.py
主要用途: 提供数据库链接、增、删、查、改等操作
使用方法：
在服务器上不能在子线程中进行connect操作，否则就会一直卡在connect处。
因此只能在主线程中先创建多个连接，存到连接池中。
当子线程需要用到连接时，从连接池中取一个，用完以后还给连接池。
由于连接长时间不使用就会中断，所以要定时使用一次每个连接。
"""

import pymysql
import re
import gc
import os
from api.log_manager import getLog
from api import config_manager
from multiprocessing import Lock
import time
import threading
import queue
from api.error_manager import DatabaseError

thread_lock = threading.Lock()
process_lock = Lock()
logger = getLog()
# 长时间连接不用，conn会自动关闭，关闭的情况下进行操作会报(0,'')异常
conn_error_re = re.compile('(?:MySQL server has gone away|Lost connection to MySQL server during query|\(0, \'\'\))')

# 将当前进程id写到文件，守护进程要使用。
sys_ppid = os.getppid()


class Database(object):
    """
    数据库连接池
    """
    _instance_lock = Lock()

    @classmethod
    def get_db(cls, *args, **kwargs):
        # 加锁形式的单例模式，保证SearchEngine._instance只会初始化一次
        if not hasattr(Database, "_instance"):
            with Database._instance_lock:
                if not hasattr(Database, "_instance"):
                    Database._instance = Database(*args, **kwargs)
        return Database._instance

    def __init__(self, *args, **kwargs):
        self.is_exit = False
        self.conn_count = 0
        try:
            self.db_name = args[0]
        except:
            self.db_name = None
        try:
            # 连接池的大小
            conn_size = int(config_manager.get_config_values('mysql', 'conn_size'))
        except:
            logger.warning('conn_size读取配置失败，使用默认值')
        try:
            # 连接等待的超时秒数，等待超时后关闭连接
            self.connection_wait_timeout = int(config_manager.get_config_values('mysql', 'conn_wait_timeout'))
        except:
            self.connection_wait_timeout = 3600
        try:
            # 设置时间间隔，定时对每个连接查询一次，防止连接长时间不用而中断。
            self.ping_interval = int(config_manager.get_config_values('mysql', 'ping_interval'))
        except:
            self.ping_interval = 60
        self.db_queue = queue.Queue(conn_size)
        self.db_list = []
        self.thread_connection = {}
        self.last_use_time = 0
        self.conn_use_return = True  # 一个线程完成一个任务以后就把连接归还给连接池。False表示直到线程被杀掉才归还连接，这种情况下会不断的新建连接
        if self.conn_use_return:
            # 为了在程序正常运行过程中不再新建连接，所以在最初启动时新建好连接。
            for i in range(self.db_queue.maxsize):
                try:
                    conn = DatabaseConnection(self.db_name)
                    self.db_queue.put(conn, block=False)
                    self.db_list.append(conn)
                    self.conn_count += 1
                except:
                    break
            logger.info('线程%s：连接池初始化完成，现在连接池中有%s个连接' % (threading.currentThread().getName(), self.db_queue.qsize()))
        else:
            thread_target = self.wait_close
            t = threading.Thread(target=thread_target)
            t.setName('[监控数据库]')
            t.setDaemon(True)
            t.start()

    def close_db(self):
        self.is_exit = True
        logger.info('线程%s：数据库连接即将关闭' % (threading.currentThread().getName()))
        exit(0)

    def auto_ping(self):
        """
        定时对每个连接查询一次，防止连接长时间不用而中断。
        :return:
        """
        thread_name = threading.currentThread().getName()
        sleep_first = True
        try:
            while not self.is_exit:
                # 正常使用连接后更改下次的间隔，保证每次ping都与上一次使用连接间隔ping_interval秒。
                if sleep_first:
                    time.sleep(self.ping_interval)
                if time.time() - self.last_use_time > self.ping_interval - 2:
                    for conn in self.db_list:
                        conn.set_thread_name(thread_name)
                        if conn.ping():
                            logger.debug('线程%s：ping数据库%s成功' % (thread_name, conn.name))
                        else:
                            logger.warning('线程%s：ping数据库%s失败' % (thread_name, conn.name))
                    sleep_first = True
                else:
                    time.sleep(self.ping_interval - time.time() + self.last_use_time)
                    sleep_first = False
        except SystemExit:
            logger.info('程序退出')
        except:
            logger.exception('线程%s：ping数据库遇到问题' % (thread_name))

    def wait_close(self):
        """
        定时检测是否有闲置的连接，闲置超时后关闭连接。
        由于服务器上不能正常连接，所以这里不能将数据库连接关闭。
        :return:
        """
        while not self.is_exit:
            time.sleep(5)
            thread_list = [t.getName() for t in threading.enumerate()]
            # 遍历线程已分配连接的字典，如果线程已关闭，则回收该线程的连接。
            for th in self.thread_connection.keys():
                if th not in thread_list:
                    self.put_connenction(self.thread_connection.pop(th))
            if not self.db_queue.empty() and self.last_use_time != 0 and time.time() - self.last_use_time > self.connection_wait_timeout:
                # 如果数据库连接已经有
                try:
                    self.db_queue.get(True,1)
                    logger.debug(
                        '数据库连接闲置%s秒，已关闭一个最久未使用的连接，连接池中剩余%s个连接' % (self.connection_wait_timeout, self.db_queue.qsize()))
                except queue.Empty:
                    pass
                except:
                    logger.exception('定时关闭数据库连接出现异常')

    def get_connection(self, try_count=1):
        """
        线程从连接池中取连接
        :param try_count: 用于多次尝试的计数
        :return:
        """
        self.last_use_time = time.time()
        thread_name = threading.currentThread().getName()
        if thread_name in self.thread_connection:
            # 如果线程已经分配了连接，直接返回。
            # 当线程用完连接时要注意归还连接，以及删除thread_connection中的线程-连接映射
            return self.thread_connection[thread_name]
        if not self.conn_use_return and self.db_queue.empty():
            # 用完连接就归还的模式下，不会新建连接
            # 自动回收连接的模式下，要新建连接
            logger.debug('线程%s：没有可用连接' % (thread_name))
            db = DatabaseConnection()
            self.db_queue.put(db)
        try:
            result = self.db_queue.get(True,0.1)
            result.set_thread_name()
            self.thread_connection[thread_name] = result
            logger.debug('线程%s：取得数据库%s，连接池中剩余%s个连接' % (thread_name, result.name, self.db_queue.qsize()))
        except:
            if try_count < 3:
                logger.warning('线程%s：第%s次取数据库连接失败' % (thread_name, try_count))
                return self.get_connection(try_count + 1)
            else:
                logger.error('线程%s：第%s次取数据库连接失败' % (thread_name, try_count))
                raise DatabaseError('连接池中无可用的数据库连接')
        return result

    def return_thread_conn(self, thread_name=None):
        """
        归还这个线程分配到的连接
        :param thread_name:
        :return:
        """
        if thread_name is None:
            thread_name = threading.currentThread().getName()
        if thread_name in self.thread_connection:
            self.put_connenction(self.thread_connection.pop(thread_name))

    def put_connenction(self, connection):
        """
        归还连接到连接池中
        :param connection:
        :return:
        """
        db_queue = self.db_queue
        if not db_queue.full():
            db_queue.put(connection)
            logger.debug(
                '线程%s：归还数据库%s，连接池中剩余%s个连接' % (threading.currentThread().getName(), connection.name, db_queue.qsize()))

    def fetch_all(self, sql):
        db = self.get_connection()
        result = db.fetch_all(sql)
        return result

    def fetch_one(self, sql):
        db = self.get_connection()
        result = db.fetch_one(sql)
        return result

    def execute_sql(self, sql):
        db = self.get_connection()
        db.execute_sql(sql)

    def truncate_table(self, table):
        db = self.get_connection()
        db.truncate_table(table)

    def call_proce(self, proce_name, value):
        db = self.get_connection()
        db.call_proce(proce_name, value)


class DatabaseConnection(object):
    """
    基本描述：数据库工具类
    详细描述：提供数据库的开启连接、关闭连接、清空表格、获取执行结果列表、执行sql语句等基本操作
    属性说明：self.host 主机地址,
            self.port 端口号,
            self.user 数据库用户名,
            self.passwd 数据库密码,
            self.db_name 数据库名称,
            self.charset 编码采用的字符集,
            self.conn 数据库连接对象
            self.cur 数据库操作游标
    """

    def __init__(self, db_name, conn_count=0):
        """
        数据库初始化，从配置文件读取主机名、端口号、用户名、密码、数据库名称、字符编码等数据库连接相关信息
        """
        self.conn = None
        self.db_host = config_manager.get_config_values('mysql', 'host')
        self.db_port = config_manager.get_config_values('mysql', 'port')
        self.db_user = config_manager.get_config_values('mysql', 'user')
        self.db_passwd = config_manager.get_config_values('mysql', 'password')
        if db_name is None or len(db_name)<1:
            self.db_name = config_manager.get_config_values('mysql', 'database_AI')
        else:
            self.db_name = db_name
        self.db_charset = config_manager.get_config_values('mysql', 'charset')
        self.connect_db()  # 连接数据库
        self.last_exe_time = 0  # 上一次进行数据库操作的时间
        self.name = '<连接%s>' % conn_count

    def set_thread_name(self, thread_name=None):
        if thread_name is None:
            self.thread_name = threading.currentThread().getName()
        else:
            self.thread_name = thread_name

    def connect_db(self):
        """
        连接数据库
        :return self.conn: 数据库连接对象
        :return self.cur: 数据库操作游标
        """
        self.set_thread_name()
        try:
            self.conn.close()
            logger.info('线程%s：数据库连接已主动中断' % self.thread_name)
        except Exception as  e:
            if self.conn is None:
                logger.info('线程%s：正在初始化数据库连接...' % self.thread_name)
            elif str(e) == 'Already closed':
                logger.info('线程%s：数据库连接已异常中断' % self.thread_name)
            else:
                logger.exception('线程%s：中断数据库连接时出现异常' % self.thread_name)
                del self.conn
                gc.collect()
        conn_success = False
        max_reconnect_time = 3
        reconnect_count = 1
        while reconnect_count <= max_reconnect_time and not conn_success:
            logger.info('线程%s：第%s次尝试连接数据库...' % (self.thread_name, reconnect_count))
            self.conn = pymysql.connect(
                self.db_host,
                self.db_user,
                self.db_passwd,
                self.db_name,
                int(self.db_port),
                autocommit=True,  # 防止数据查询时查不到最新的数据，跟事务隔离有关。http://www.bkjia.com/Pythonjc/1228355.html
                connect_timeout=2,
                charset=self.db_charset
            )
            try:
                logger.info(self.conn.host_info)
                logger.info('线程%s：数据库连接成功' % self.thread_name)
                self.cur = self.conn.cursor()
                conn_success = True
            except:
                logger.exception('线程%s：数据库连接失败' % self.thread_name)
                reconnect_count += 1
        if conn_success:
            logger.info('线程%s：初始化数据库完成' % self.thread_name)
        else:
            raise DatabaseError('线程%s：初始化数据库失败' % self.thread_name)
        return self.conn, self.cur

    def __del__(self):
        self.close_db()

    def close_db(self):
        """
        关闭数据库连接
        :return:
        """
        print('线程%s：正在关闭数据库连接...' % self.thread_name)
        return self.conn.close()

    def truncate_table(self, table_name):
        """
        清空表格内容
        :param table_name: 数据库表格名称
        :return:
        """
        sql = 'truncate ' + table_name
        self.execute_retry(sql)
        self.conn.commit()

    def call_proce(self, proce_name, value):
        """
        调用存储过程
        :param proce_name:存储过程的名字
        :param value: 传入的参数
        """
        result = ''
        self.cur.callproc(proce_name, (value, result))
        self.conn.commit()

    def ping(self):
        result = self.fetch_one('select id from file limit 1')
        if result is not None and len(result) > 0:
            return True
        return False

    def fetch_all(self, sql):
        """
        获取所有的返回结果列表
        :param sql: 需要执行的sql语句
        :return: 执行结果列表
        """
        self.execute_retry(sql)
        return self.cur.fetchall()

    def fetch_one(self, sql):
        """
        单条记录查询，只有一行结果
        :param sql: 需要执行的sql语句
        :return: 执行结果行
        """
        self.execute_retry(sql)
        return self.cur.fetchone()

    def execute_sql(self, sql):
        """
        执行sql语句
        :param sql: 需要执行的sql语句
        :return:
        """
        self.execute_retry(sql)
        self.conn.commit()

    def execute_retry(self, sql, retry_count=0):
        """
        重复执行sql语句
        :param sql: 需要执行的sql语句
        :param retry_count: 重复的次数
        :return:
        """
        try:
            cur_ppid = os.getppid()
            # 多进程和多线程中要使用不同的锁
            if cur_ppid != sys_ppid:
                lock = process_lock
                pname = '进程%s' % os.getpid()
            else:
                lock = thread_lock
                pname = '线程%s' % self.thread_name
            # logger.debug('%s：请求数据库锁' % pname)
            with lock:
                # logger.debug('%s：获得数据库锁' % pname)
                self.cur.execute(sql)
                # logger.debug('%s：释放数据库锁' % pname)
        except pymysql.Error as e:
            # 要进行重连，否则长时间不用mysql也会超时，导致出错
            err_str = str(e)
            logger.warning('线程%s：数据库连接出现故障，故障信息：%s\nsql：%s' % (self.thread_name, err_str, sql))
            if self.thread_name == 'MainThread' and conn_error_re.search(err_str):
                # 只在主线程中才进行重连
                if retry_count < 2:
                    num = retry_count + 1
                    logger.warning('线程%s：正在进行第%s次重连' % (self.thread_name, num))
                    self.connect_db()
                    self.execute_retry(sql, num)
                else:
                    logger.error('线程%s：数据库重连3次仍然出现故障，程序即将退出!' % self.thread_name)
                    raise DatabaseError('数据库无法进行连接')
            elif 'syntax' in err_str:
                logger.exception('线程%s：数据库操作出现异常，sql：%s' % (self.thread_name, sql))
            else:
                raise DatabaseError('数据库%s不可用' % (self.name))
        except:
            logger.exception('线程%s：数据库操作出现异常，sql：%s' % (self.thread_name, sql))

# def conn_work(i):
#     conn = DatabaseConnection()
#     a = conn.fetchone('select * from file limit 1')
#     if a is not None and len(a) > 0:
#         print('子线程第%s次测试连接成功' % i)
#
#
# def pool_work(pool, i):
#     db = pool
#     a = db.fetchone('select * from file limit 1')
#     if a is not None and len(a) > 0:
#         print('连接池同时第%s次测试连接成功' % i)
#     db.return_thread_conn()
#
#
# def pool_new(pool, i):
#     pool.db_queue.get()
#     print('连接池创建连接测试，取出连接，现有连接数%s' % pool.db_queue.qsize())
#     db = DatabaseConnection()
#     pool.db_queue.put(db)
#     print('连接池创建连接测试，存入连接，现有连接数%s' % pool.db_queue.qsize())
#
#
# def test_conn():
#     print('测试数据库连接开始...')
#     for i in range(1, 5):
#         conn = DatabaseConnection()
#         a = conn.fetchone('select * from file limit 1')
#         if a is not None and len(a) > 0:
#             print('主线程第%s次测试连接成功' % i)
#
#     print('主线程测试数据库连接结束')
#     for i in range(1, 5):
#         t = threading.Thread(target=conn_work, args=(i,))
#         t.setName('[无池-%s]' % i)
#         t.start()
#     pool = Database().get_db()
#     for i in range(1, 5):
#         db = pool
#         a = db.fetchone('select * from file limit 1')
#         if a is not None and len(a) > 0:
#             print('连接池第%s次测试连接成功' % i)
#
#     print('主线程连接池测试数据库连接结束')
#     for i in range(1, 5):
#         t = threading.Thread(target=pool_work, args=(pool, i))
#         t.setName('[连接池使用-%s]' % i)
#         t.start()
#
#     print('连接池测试数据库连接使用结束')
#     print('连接池测试数据库连接创建开始')
#     for i in range(1, 5):
#         t = threading.Thread(target=pool_new, args=(pool, i))
#         t.setName('[连接池创建-%s]' % i)
#         t.start()
#
#
# if __name__ == '__main__':
#     test_conn()
