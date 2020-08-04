import os, re, time, random

myopen = lambda i, j: open(i, j, encoding='utf-8')


def get_sys_type():
    """
    posix表示类Unix或OS X系统。nt表示windows系统
    :return:
    """
    if os.name == 'nt':
        return 1
    else:
        return 0


def get_project_path():
    """
    基本描述：获取工程路径
    详细描述：获取本工程路径，首先获取当前utils.py文件的绝对路径，向上两级找到工程路径。
    属性说明：
    Returns：获取本工程路径字符串
    """
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))








def get_current_date():
    now = int(time.time())
    # 转换为其他日期格式,如:"%Y-%m-%d %H:%M:%S"
    timeStruct = time.localtime(now)
    return time.strftime("%Y-%m-%d", timeStruct)


