# 实时K线：https://openapi.futunn.com/futu-api-doc/quote/get-kl.html
# 历史K线：https://openapi.futunn.com/futu-api-doc/quote/request-history-kline.html
from futu import *

STOCK_CODES = [
    'HK.00700',  # 腾讯控股
    'HK.01024',  # 快手-W
    'HK.03690',  # 美团-W
    'HK.09988',  # 阿里巴巴-W
    'HK.01810'   # 小米集团-W
]

quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
ret_sub, err_message = quote_ctx.subscribe(['HK.00700'], [SubType.K_DAY], subscribe_push=False, session=Session.ALL)
# 先订阅 K 线类型。订阅成功后 OpenD 将持续收到服务器的推送，False 代表暂时不需要推送给脚本
if ret_sub == RET_OK:  # 订阅成功
    ret, data = quote_ctx.get_cur_kline('HK.00700', 10, KLType.K_DAY, AuType.QFQ)  # 获取港股00700最近10个 K 线数据
    if ret == RET_OK:
        print(data)
        data.to_json('./data/hk_00700_kline.jsonl', orient='records', lines=True, force_ascii=False)
    else:
        print('error:', data)
else:
    print('subscription failed', err_message)
quote_ctx.close()  # 关闭当条连接，OpenD 会在1分钟后自动取消相应股票相应类型的订阅

