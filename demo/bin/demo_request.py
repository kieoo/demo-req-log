#!/bin/python
import boto3
import time
import datetime
import gzip
import os
# import sys
# sys.path.append('./')
# print(sys.path)
from demo.config import config

s3_resource = boto3.resource('s3')

demo_up_resp_map = {}
request_id_list = {}


def get_s3_bucket(region=None):
    global s3_resource
    if not region or s3_resource.meta.client.meta.region_name == region:
        return s3_resource.Bucket(config.bucket_name)
    else:
        return boto3.resource('s3', region_name=region).Bucket(config.bucket_name)


def yield_log(timestamp, region):
    """
    :param timestamp: int
    :param region: string
    :return:
    """
    # 拼接目录
    date = time.strftime("%Y/%m/%d", time.localtime(timestamp))
    hour = time.strftime("%H", time.localtime(timestamp))
    dir_name = "{0}/{1}/{2}/{3}".format(config.s3_dir_name, date, region, hour)
    print(">>> scan log in %s" % dir_name)

    # 文件列表
    s3_resource_summary = get_s3_bucket().objects.filter(Prefix=dir_name)

    # 循环每个文件内容
    for file_name in s3_resource_summary:
        print(">>> read log in %s" % file_name.key)
        # 流式请求
        resp = get_s3_bucket().Object(file_name.key).get()
        # 解压
        ungzip = gzip.decompress(resp.get("Body").read()).decode("utf-8")

        for log_line in ungzip.splitlines():
            # 一个yield 日志流
            yield log_line


def mark_down(mark_demo_up_resp_map, log_dir_name, timestamp):
    """
    :param mark_demo_up_resp_map:
    :param log_dir_name
    :param timestamp int
    :return:
    """
    date = time.strftime("%Y-%m-%d", time.localtime(timestamp))

    for group_name, req_bodys in mark_demo_up_resp_map.items():
        channel_dsp_adtype = group_name.split("_")
        # channel_dsp 作为文件名
        dir_name = os.path.join(log_dir_name, channel_dsp_adtype[0], channel_dsp_adtype[1], date)

        for request_id, req_body_list in req_bodys:
            # 找齐了
            if isinstance(req_body_list, list) and len(req_body_list) >= 4:
                os.makedirs(dir_name, exist_ok=True)
                file_name = os.path.join(dir_name, group_name + "_" + request_id + ".txt")
                print("mark down request_id:", request_id)
                print("file name:", file_name)
                with open(file_name, "w+") as f:
                    for req_body in req_body_list:
                        f.write('\n')
                        f.write("-----------------分割线-------up: 上游流量 down: 下游流量-------req: 请求 resp: 响应---------------------------------" + '\n')
                        f.write('\n')
                        f.write(req_body + '\n')


def up_resp(timestamp, region):
    """
    :param timestamp: 时间
    :param region: 地区
    :return: None 修改全局demo_up_resp_map
    """
    global demo_up_resp_map
    global request_id_list
    # 只查找上游+resp的requesetId
    for log in yield_log(timestamp, region):
        log_filed_v = log.split('\t')
        # 只有 上游的resp才放进来
        if len(log_filed_v) < 12:
            print(">>>err log:", log_filed_v)
            continue
        if log_filed_v[10] != 'up' or log_filed_v[11] != 'resp':
            continue
        # 开始筛选
        i = 0
        resp_up_map = {}
        # 解析up_resp日志
        for v in log_filed_v:
            resp_up_map[config.log_filed[i]] = v
            i += 1
        up_unique = ""

        # 拼接
        for uniqu_key in config.group_by:
            # channel_dsp_adtype_region
            if len(up_unique) == 0:
                up_unique = str(resp_up_map.get(uniqu_key, " "))
            else:
                if uniqu_key == "ad_type":
                    # 转adtype 为 string
                    adtype = int(resp_up_map.get(uniqu_key))
                    up_unique = up_unique + "_" + str(config.adtype_map.get(adtype, adtype))
                else:
                    up_unique = up_unique + "_" + str(resp_up_map.get(uniqu_key, " "))

        # append requestid 进 demo_up_resp_map
        if up_unique not in demo_up_resp_map:
            demo_up_resp_map[up_unique] = []
        if up_unique in demo_up_resp_map and len(demo_up_resp_map.get(up_unique)) < config.uniq_count:
            request_point = []
            demo_up_resp_map[up_unique].append((resp_up_map.get("request_id", ""), request_point))

            # 做个指向demo_up_resp_map request的指针
            request_id_list[resp_up_map.get("request_id", "")] = (up_unique, request_point)


def scan_log(timestamp, region):
    """
    :param time:
    :param region:
    :return: 存储原始日志
    """
    for log in yield_log(timestamp, region):
        log_filed_v = log.split('\t')
        if len(log_filed_v) < 12:
            print(">>>err log:", log_filed_v)
            continue
        # 只有存在在request_id_list的requestid 才记录
        if log_filed_v[2] in request_id_list:
            # 下游不是胜出dsp的, 不写入
            if log_filed_v[10] == 'down':
                log_dsp = log_filed_v[9]
                save_dsp = request_id_list[log_filed_v[2]][0].split("_")[1]
                if log_dsp == save_dsp:
                    request_id_list[log_filed_v[2]][1].append(log)
            # 上游的都写入
            else:
                request_id_list[log_filed_v[2]][1].append(log)


if __name__ == "__main__":
    # global demo_up_resp_map
    # global request_id_list

    # 整点
    hour_now = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    timestamp_now = int(time.mktime(hour_now.timetuple()))
    # 上个小时
    log_timestamp = timestamp_now - 3600

    for region in config.regions:
        demo_up_resp_map.clear()
        request_id_list.clear()
        up_resp(log_timestamp, region)
        scan_log(log_timestamp, region)
        mark_down(demo_up_resp_map, config.mark_down_dir, timestamp_now)
