
# About
实现了两个功能：
1. python端向手机端的企业微信发送消息，可以实现定时检查服务器运行状态，或者实时发送报警消息。
2. 手机端的企业微信向python端发送消息，python端作出应答，可以实现查询服务器状态，或者其他关心的内容。
# 配置文件
我这里配置了两个应用，每个应用都有各自的id和secret：
1. CORP_ID是企业微信的企业id，可以在企业微信的后台查看
2. SYS_APP_ID是“服务器监控”应用的应用id，企业微信的后台中的应用管理中可以看到对应的名称为AgentId
3. SYS_APP_SECRET是“服务器监控”应用的应用密钥，对应名称是Secret
4. REQUEST_TOKEN是“用户使用情况”应用的接收消息API生成的Token
5. REQUEST_EncodingAESKey“用户使用情况”应用的接收消息API生成的EncodingAESKey
6. 数据库配置和业务相关，可自行决定是否保留。
# 接收消息
企业微信后台，进入自建的应用管理页面，点击“接收消息”框中的“设置API接收”，随机获取Token和EncodingAESKey，记下来，会用到。url需要设置为python端开启的服务端接口，如我这里设置为
http://ip:5555/wxpush/
相应的flask要设置为：@app.route('/wxpush/', methods=['GET', 'POST'])，注意斜杠和post都不能少。