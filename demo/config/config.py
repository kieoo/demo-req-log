#!/bin/python

bucket_name = "mob-ad"

# file name: "adn/adx-v1/raw/2021/11/28/seoul/23/.*gz"
s3_dir_name = "adn/adx-v1/raw"

mark_down_dir = "/tmp/req_demo"

regions = ["frankfurt", "seoul", "singapore", "virginia"]

log_filed = [
    'data_time',
    'timestamp',
    'request_id',
    'channel_id',
    'is_hb',
    'tag_id',
    'ad_type',
    'country_code',
    'region',
    'dsp_id',
    'flow_type',
    'msg_type',
    'raw_body'
]

group_by = ['channel_id', 'dsp_id', 'ad_type', "region"]

# 重复条数
uniq_count = 1

adtype_map = {
    12: "reward-video",
    13: "native-video",
    14: "interstitial-video",
    15: "native-image",
    16: "interactive-ads",
    17: "banner",
    18: "instream-video",
    19: "splash",
    20: "display-interstitial",
    21: "native-h5"
}
