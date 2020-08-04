# -*- coding: utf-8 -*-

'''
Created on 2018-7-4
修改于2018-09-27，李翔
主要用途：
    对程序中所使用的loggong模式做一般性配置
@author: luoqixing

代码使用：
可能需要使用utf-8编码

import LogManager

logger = getLog()



logger.debug(msg)
logger.info(msg)
logger.warn(message)
logger.error(msg)
logger.critical(msg)//尽量避免使用这个,错误等级使用到error就行,使用该等级控制控制台日志的输出

多个参数使用方式logger.info("{} {} ...".format(arg1,arg2,...))
'''

import logging

import _locale

_locale._getdefaultlocale = (lambda *args: ['en_US', 'utf8'])

import logging.handlers
from multiprocessing import Lock
import os
from api.config_manager import get_config_values
from api.help import get_project_path

from concurrent_log_handler import ConcurrentRotatingFileHandler


class LogManager(object):
    _instance_lock = Lock()

    def __init__(self):
        try:
            logs_dir = get_config_values("python-log", "dir")
        except:
            logs_dir = os.path.join(get_project_path(), 'logs')
        try:
            f_levl = get_config_values("python-log", "f_level")
        except:
            f_levl = 'DEBUG'
        try:
            c_levl = get_config_values("python-log", "ch_level")
        except:
            c_levl = 'DEBUG'
        if os.path.exists(logs_dir) and os.path.isdir(logs_dir):
            pass
        else:
            os.mkdir(logs_dir)
        # 单个日志最大体积到了限制以后新建一个日志，并重命名旧的日志
        maxFileSize = int(get_config_values("python-log", "maxBytes"))
        # 日志总数到了限制以后删除最旧的日志
        backUp = int(get_config_values("python-log", "backupCount"))
        # 设置控制台的输出级别，与文件输出区分开

        formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        self.file_logger = logging.getLogger("file_logger")
        self.file_logger.setLevel(eval('logging.' + f_levl))
        console = logging.StreamHandler()
        console.setLevel(eval('logging.' + c_levl))
        console.setFormatter(formatter)
        # 控制台将包含非本项目的logging的输出
        logging.getLogger("").addHandler(console)

        # 将调试和信息输出到debug日志中
        debug_file_name = os.path.join(logs_dir, 'ai_writer.log')
        debug_rotatingFileHandler = ConcurrentRotatingFileHandler(debug_file_name, 'a', maxFileSize, backUp, 'utf-8')
        debug_rotatingFileHandler.setFormatter(formatter)
        self.file_logger.addHandler(debug_rotatingFileHandler)

        print("初始化logger完成")

    @classmethod
    def get_logger(cls, *args, **kwargs):
        # 加锁形式的单例模式，Loggers._instance只会初始化一次
        if not hasattr(LogManager, "_instance"):
            with LogManager._instance_lock:
                if not hasattr(LogManager, "_instance"):
                    LogManager._instance = LogManager(*args, **kwargs)
        return LogManager._instance


# 获取log对象,返回类型是logging
def getLog():
    return LogManager.get_logger().file_logger


if __name__ == "__main__":
    logger = getLog()
    import traceback

    for i in range(1000):
        logger.debug('测试debug')
        logger.info('测试info')
        logger.warning('测试warning')
        logger.error('测试error')
        try:
            a = 3 / 0
        except:
            traceback.print_exc()
        try:
            a = []
            b = a[1]
        except:
            logger.exception('测试exception2')
