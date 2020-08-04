import os, sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from api.log_manager import getLog
from api.error_manager import DatabaseError
from api.database_manager import Database
from runtime.time_mission import TimeMission
from api.WXBizMsgCrypt import WXBizMsgCrypt
from api.config_manager import get_config_values
# 导入Flask类
from flask import Flask, request
from xml.etree import ElementTree as ET

# 实例化，可视为固定格式
app = Flask(__name__)
logger = getLog()

# route()方法用于设定路由；类似spring路由配置
@app.route('/wxpush/', methods=['GET', 'POST'])
def wxpush():
    logger.debug('收到消息：%s' % 'wxpush')
    # 获取url验证时微信发送的相关参数
    sVerifyMsgSig = request.args.get('msg_signature')
    sVerifyTimeStamp = request.args.get('timestamp')
    sVerifyNonce = request.args.get('nonce')
    sVerifyEchoStr = request.args.get('echostr')
    sReqMsgSig = sVerifyMsgSig
    sReqTimeStamp = sVerifyTimeStamp
    sReqNonce = sVerifyNonce
    # 验证url
    if request.method == 'GET':
        ret, sEchoStr = wxcpt.VerifyURL(sVerifyMsgSig, sVerifyTimeStamp, sVerifyNonce, sVerifyEchoStr)
        logger.debug('验证wxpush的url成功：%s' % sEchoStr)
        if (ret != 0):
            logger.error("ERR: VerifyURL ret:" + ret)
        return sEchoStr
    # 接收客户端消息 post请求
    sReqData = request.data
    ret, sMsg = wxcpt.DecryptMsg(sReqData, sReqMsgSig, sReqTimeStamp, sReqNonce)
    if (ret != 0):
        logger.error("ERR: VerifyURL ret:")
    # 解析发送的内容并打印
    dMsg = sMsg.decode('utf8')
    logger.debug(dMsg)
    xml_tree = ET.fromstring(dMsg)
    content = xml_tree.find("Content").text
    ToUserName = xml_tree.find("ToUserName").text
    FromUserName = xml_tree.find("FromUserName").text
    CreateTime = xml_tree.find("CreateTime").text
    MsgType = xml_tree.find("MsgType").text
    MsgId = xml_tree.find("MsgId").text
    AgentID = xml_tree.find("AgentID").text
    try:
        db = Database.get_db()
        rMsg = tm.get_request_msg(content, db)
        db.return_thread_conn()
    except DatabaseError:
        rMsg = '数据库出错了'
    except:
        rMsg = '出现未知错误'
    sReqData = "<xml><ToUserName><![CDATA[%s]]></ToUserName><FromUserName><![CDATA[%s]]></FromUserName><CreateTime>%s</CreateTime><MsgType><![CDATA[%s]]></MsgType><Content><![CDATA[%s]]></Content><MsgId>%s</MsgId><AgentID>%s</AgentID></xml>" % (
        FromUserName, ToUserName, CreateTime, MsgType, rMsg, MsgId, AgentID)
    logger.debug(rMsg)
    ret, sEncryptMsg = wxcpt.EncryptMsg(sReqData, sReqNonce, sReqTimeStamp)
    if (ret != 0):
        logger.error("ERR: EncryptMsg ret:" + ret)
    logger.debug(sEncryptMsg)
    return sEncryptMsg


tm = TimeMission()
corpid = get_config_values('weixin', 'CORP_ID')
request_token = get_config_values('weixin', 'REQUEST_TOKEN')
request_aeskey = get_config_values('weixin', 'REQUEST_EncodingAESKey')
wxcpt = WXBizMsgCrypt(request_token, request_aeskey, corpid)
app.run(host="0.0.0.0", port=5555)
