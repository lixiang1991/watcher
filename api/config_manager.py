#! /usr/bin/python
# -*- coding: utf-8 -*-

"""
@author: 李翔、黄炎
@time: 2018-7-25
文件名: config_manager.py
主要用途:
    从服务器中相对当前用户home目录路径位置读取配置文件config.ini的内容
    首先，从服务器相对路径找到配置文件config.ini
    然后，解析配置文件，根据section和option值获取配置文件内容
使用方法：
    from common import config_manager
    host = config_manager.get_config_values('mysql', 'host')
"""

import os
import sys
import traceback
from api.help import get_project_path

import configparser as ConfigParser


znxz_config_path = os.path.join(get_project_path(), "config", "config.ini")


def get_config_values(section, option):
    """
    根据section和option获取配置文件中对应的值
    :param section: 配置文件中的section名称，即中括号内的值
    :param option: 配置文件中的option名称，即等号前的名称
    :return: 配置文件中section部分的option对应的值
    :raises:
        NoSectionError: 读取配置文件错误
    """
    cp = ConfigParser.ConfigParser()
    try:
        cp.read(znxz_config_path,encoding='utf8')
        return cp.get(section=section, option=option)
    except ConfigParser.NoSectionError:
        print("Read {} file error!".format(znxz_config_path))
        traceback.print_exc()
        sys.exit(1)


def set_config_values(section, option, value):
    """
    根据section、option和value写入或更新配置文件中对应的值
    :param section: 配置文件中的section名称，即中括号内的值
    :param option: 配置文件中的option名称，即等号前的名称
    :param value: 配置文件中option对应的值
    :return: 是否设置成功
    :raises:
    """
    cp = ConfigParser.ConfigParser()
    try:
        cp.read(znxz_config_path, encoding='utf8')
        cp.set(section, option, value)
        cp.write(open(znxz_config_path, 'w', encoding='utf8'))
    except:
        print("Write {} file error!".format(znxz_config_path))
        traceback.print_exc()
        sys.exit(1)
    return True

if __name__ == '__main__':
    result = get_config_values('mysql', 'port')
    print(result)
