#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import threading
import psutil
import random
import subprocess
import os
import datetime
from api.log_manager import getLog
from api.CorpApi import CorpApi, CORP_API_TYPE, ApiException
from api.config_manager import get_config_values
from api.database_manager import Database

logger = getLog()


class TimeMission(object):
    def __init__(self):
        """
        初始化服务端
        :param info: 后台主要处理程序初始化完成后得到的实例
        """
        self.is_exit = False
        corpid = get_config_values('weixin', 'CORP_ID')
        sys_appsecret = get_config_values('weixin', 'SYS_APP_SECRET')
        request_appsecret = get_config_values('weixin', 'REQUEST_APP_SECRET')
        sys_appid = get_config_values('weixin', 'SYS_APP_ID')
        request_appid = get_config_values('weixin', 'REQUEST_APP_ID')
        info_send_enable = get_config_values('info_send', 'enable') == 'true'
        request_send_enable = get_config_values('request_send', 'enable') == 'true'
        self.user_white_list = set(get_config_values('user_white', 'list').split(','))
        self.mode_list = set(get_config_values('user_white', 'modes').split(','))
        self.info_time = None
        self.request_time = None
        if info_send_enable:
            self.info_time = {'start': int(get_config_values('info_send', 'start').replace(':', '')),
                              'end': int(get_config_values('info_send', 'end').replace(':', '')),
                              'interval': int(get_config_values('info_send', 'interval').replace(':', '')),
                              'last': None}
        if request_send_enable:
            self.request_time = {'start': int(get_config_values('request_send', 'start').replace(':', '')),
                                 'end': int(get_config_values('request_send', 'end').replace(':', '')),
                                 'interval': int(get_config_values('request_send', 'interval').replace(':', '')),
                                 'last': None}
        self.shell_dir = '/home/writer_ai/'
        self.mission_list = {'elastic': {'status': 0, 'file': 'run_es.sh'},
                             'intelliwriting': {'status': 0, 'file': 'run_java.sh'},
                             'Kuaixie2': {'status': 0, 'file': 'run_python.sh'},
                             'redis': {'status': 0, 'file': 'run_redis.sh'},
                             'nginx': {'status': 0, 'file': 'run_nginx.sh'},
                             'mysql': {'status': 0, 'file': 'run_mysql.sh'}}
        self.last_wx_msg = None
        self.sys_api = CorpApi(corpid, sys_appsecret, sys_appid)
        self.request_api = CorpApi(corpid, request_appsecret, request_appid)
        self.status_dict = None
        self.host_whites = ['113.57.119.233', '', '0.0.0.0']
        self.printed_user = []
        t = threading.Thread(target=self.record_work)
        t.setDaemon(True)
        t.setName('[监控任务]')
        t.start()

    def can_send_msg(self, mission_type='sys'):
        if mission_type == 'sys':
            mission_dict = self.info_time
        else:
            mission_dict = self.request_time
        if mission_dict is None:
            return False
        time_now = int(time.strftime('%H%M%S'))
        if time_now < mission_dict['start'] or time_now > mission_dict['end']:
            mission_dict['last'] = None
            return False
        if mission_dict['last'] is not None and time_now < mission_dict['last'] + mission_dict['interval']:
            return False
        else:
            mission_dict['last'] = time_now
            return True

    def get_sys_status(self, interval=10):
        tot_before = psutil.net_io_counters()
        cpu_status = psutil.cpu_percent(interval)
        phymem = psutil.virtual_memory()
        memory_status = phymem.percent
        storage_status = psutil.disk_usage('/').percent

        tot_after = psutil.net_io_counters()
        bytes_sent_per_sec = self.bytes2human((tot_after.bytes_sent - tot_before.bytes_sent) / interval)
        bytes_recv_per_sec = self.bytes2human((tot_after.bytes_recv - tot_before.bytes_recv) / interval)
        packets_sent_per_sec = (tot_after.packets_sent - tot_before.packets_sent) / interval
        packets_recv_per_sec = (tot_after.packets_recv - tot_before.packets_recv) / interval
        login_users = psutil.users()
        for user in login_users:
            if user.host not in self.host_whites and user.host not in self.printed_user:
                self.send_to_wx('有用户登录了服务器，%s，%s' % (user.host, user.name))
                self.printed_user.append(user.host)
        for pr_user in self.printed_user:
            found = False
            for user in login_users:
                if pr_user == user.host:
                    found = True
            if not found:
                self.printed_user.remove(pr_user)
                self.send_to_wx('有用户退出了服务器，%s' % (pr_user))
        levels = {'严重': 90, '警告': 70}
        for text, num in levels.items():
            if cpu_status > num or memory_status > num or storage_status > num:
                msg = '[%s]cpu:%s%%  mem:%s%%  hd:%s%%  send:%s/s,%s  recv:%s/s,%s' % (
                    text, cpu_status, memory_status, storage_status, bytes_sent_per_sec, packets_sent_per_sec,
                    bytes_recv_per_sec, packets_recv_per_sec)
                logger.debug(msg)
                self.send_to_wx(msg)
                break
        self.status_dict = {'cpu_status': cpu_status, 'memory_status': memory_status, 'storage_status': storage_status,
                            'bytes_sent_per_sec': bytes_sent_per_sec, 'bytes_recv_per_sec': bytes_recv_per_sec,
                            'packets_sent_per_sec': packets_sent_per_sec, 'packets_recv_per_sec': packets_recv_per_sec}

    def send_healthy_msg(self):
        if self.status_dict is None:
            return None
        msg = '系统运行正常，cpu:%s%%' % (self.status_dict['cpu_status'])
        self.send_to_wx(msg)

    def bytes2human(self, n):
        """
        #>>>bytes2human(10000)
        '9.8k'
        #>>>bytes2human(100001221)
        '95.4M'
        """
        symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
        prefix = {}
        for i, s in enumerate(symbols):
            prefix[s] = 1 << (i + 1) * 10
        for s in reversed(symbols):
            if n >= prefix[s]:
                value = float(n) / prefix[s]
                return '%.2f%s' % (value, s)
        return '%.2fB' % (n)

    def send_to_wx(self, msg, app_api=None):
        if app_api is None:
            app_api = self.sys_api
        try:
            # 不会连续发送重复内容
            if msg == self.last_wx_msg:
                return False
            self.last_wx_msg = msg
            time0 = time.strftime('%d %H:%M:%S')
            msg = '%s - %s' % (time0, msg)
            response = app_api.httpCall(
                CORP_API_TYPE['MESSAGE_SEND'],
                {
                    "touser": "@all",
                    "agentid": app_api.appid,
                    'msgtype': 'text',
                    'climsgid': 'climsgidclimsgid_%f' % (random.random()),
                    'text': {
                        'content': msg,
                    },
                    'safe': 0,
                })
            print(response)
        except ApiException as e:
            logger.exception('微信发送消息出错了！')

    def record_work(self):
        """
        后台线程，每过一分钟，进行一次检测。
        """
        logger.info('已加载任务定时监控模块')
        while not self.is_exit:
            try:
                # 设置每分钟的秒数

                t = threading.Thread(target=self.get_sys_status)
                t.setName('[获取系统状态]')
                t.start()
                if self.can_send_msg():
                    self.send_healthy_msg()
                if self.can_send_msg('request'):
                    self.send_request_msg()
                self.check_processes_status()

                time.sleep(60)
            except:
                logger.exception('任务监控模块出错')
                break
        else:
            logger.info('任务监控模块已结束')

    def check_processes_status(self):
        try:
            error_list = []
            for m_name, mission_dict in self.mission_list.items():
                if not self.check_process_status(m_name, mission_dict):
                    error_list.append(m_name)
            if len(error_list) > 0:
                msg = '严重警告，%s进程尝试启动无效' % (','.join(error_list))
                logger.error(msg)
                self.send_to_wx(msg)
        except psutil.NoSuchProcess:
            pass
        except:
            logger.exception('检测进程状态时遇到意外错误')

    def check_process_status(self, m_name, mission_dict, try_count=1):
        mission_dict['status'] = 0
        for proc in psutil.process_iter():
            pinfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
            cmds = pinfo['cmdline']
            if cmds is None:
                continue
            for cmd in cmds:
                if m_name in cmd:
                    mission_dict['status'] = 1
                    return True
        if mission_dict['status'] == 0 and try_count <= 3:
            self.try_run_mission(m_name, try_count)
            time.sleep(3 * try_count)
            self.check_process_status(m_name, mission_dict, try_count + 1)
        else:
            return False

    def try_run_mission(self, name, try_count):
        sh_path = os.path.join(self.shell_dir, self.mission_list[name]['file'])
        if not os.path.exists(sh_path):
            logger.error('shell文件不存在：%s' % sh_path)
            return False
        if try_count > 1:
            self.send_to_wx('%s进程未检测到，第%s次尝试启动...' % (name, try_count))
        result = subprocess.call(['/bin/bash', sh_path])
        return result

    def send_request_msg(self):

        db = Database.get_db()
        msg = self.get_request_msg('#24', db)
        db.return_thread_conn()
        self.send_to_wx(msg, self.request_api)

    def get_request_msg(self, recv_msg, db):
        request_type = None
        hours = 24
        time_sql = ''
        name_sql = ''
        if len(recv_msg) > 0:
            if recv_msg[0] == '#':
                try:
                    hours = int(recv_msg[1:])
                    request_type = 'hours'
                except:
                    return '参数错误：请输入#数字'
            elif recv_msg[0] == '@':
                n = recv_msg[1:]
                sql = "SELECT name from intelli_writing.t_user b where b.name='%s'" % n
                row = db.fetch_one(sql)
                if row is not None and len(row) > 0:
                    request_type = 'user'
                    name_sql = "and b.name='%s'" % n
                    hours = 0
                else:
                    return '用户[%s]不存在' % n
        if request_type is None or recv_msg in ['帮助', 'help']:
            return '查询指定用户的使用情况：@用户名；查询最近n个小时的所有用户使用情况：#n'
        if hours > 0:
            min_time = (datetime.datetime.now() - datetime.timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
            time_sql = "and unix_timestamp(send_time)>unix_timestamp('%s')" % min_time

        sql = "SELECT a.mode,a.send_time,a.send_num,a.seconds,b.name FROM db_writer_ai.mission a,intelli_writing.t_user b where a.user_name=b.username %s %s order by recv_time desc;" % (
            time_sql, name_sql)
        results = db.fetch_all(sql)
        user_count_dict = {}
        all_count = 0
        user_num = 0
        for row in results:
            user_name = row[4]
            if request_type != 'user' and user_name in self.user_white_list:
                continue
            mode = row[0]
            all_count += 1
            if user_name in user_count_dict:
                user_count_dict[user_name][mode] += 1
            else:
                user_count_dict[user_name] = {mode: 1, 'last_time': row[1]}
                for m in self.mode_list:
                    if m in user_count_dict[user_name]:
                        continue
                    user_count_dict[user_name][m] = 0
        max_user = {}
        for user_name, values in user_count_dict.items():
            user_num += 1
            user_count = 0
            for mode, count in values.items():
                if mode in self.mode_list:
                    user_count += count
            if 'user_name' not in max_user:
                max_user['user_name'] = user_name
                max_user['count'] = user_count
                max_user['values'] = values
            else:
                if user_count > max_user['count']:
                    max_user['user_name'] = user_name
                    max_user['count'] = user_count
                    max_user['values'] = values
        msg_list = []
        if hours > 0:
            msg_list.append('最近%s小时有%s个用户使用了%s次推荐' % (hours, user_num, all_count))
        if user_num > 0:
            user_msg = '用户[%s]使用了' % max_user['user_name']
            use_list = []
            for mode, count in max_user['values'].items():
                if mode in self.mode_list:
                    if count <= 0:
                        continue
                    use_list.append('%s次%s推荐' % (count, replace_mode(mode)))
            for mode, count in max_user['values'].items():
                if mode == 'last_time':
                    use_list.append('最后一次使用时间为%s' % count)
            if len(use_list) > 0:
                user_msg += '，'.join(use_list)
                msg_list.append(user_msg)
            else:
                msg_list.append('用户[%s]暂未使用' % max_user['user_name'])
        if len(msg_list) > 0:
            msg = '，'.join(msg_list)
        else:
            msg = '获取数据出错'
        return msg


def replace_mode(mode):
    result = 'None'
    if mode == 'short':
        result = '短句'
    elif mode == 'sentence':
        result = '整句'
    elif mode == 'outline':
        result = '大纲'
    return result


if __name__ == '__main__':
    ti = TimeMission()
