# @File  : traclus_consumer.py
# @Author: 沈昌力
# @Date  : 2018/4/17
# @Desc  : 实时航迹检测，kafka消费者，-h输入kafuka服务器地址：-h 127.0.0.1:9092，-t输入topic名称：-t test2
from kafka import KafkaConsumer
from kafka import KafkaProducer
import click
import configparser
from ClusterDetector import ClusterDetector
import json
import time
from multiprocessing import Process, Queue, Lock
import TraclusPoint
from logging.config import fileConfig
import logging
import KafkaInterface

shipDetecting = {}  # 所有当前正在监控的船舶


def recv_process(queue, topic, address):
    """
    从Kafka接收数据，放入队列
    :param queue: 队列
    :param topic: Kafka的Topic
    :param address: Kafka的地址
    :return:
    """
    consumer = KafkaInterface.Kafka_consumer(address, topic, 'group')
    message = consumer.consume_data()
    for msg in message:
        dt = eval(msg.value)
        queue.put(dt)  # 放入到任务队列中去
        print("recving thread. current queue length = %d" % (queue.qsize()))


def read_queue(queue, lock, workindex, sendtopic, trajclusters, cf):
    """
    从队列中读取数据，进行航迹检测
    :param queue: 队列
    :param lock: 进程锁
    :param workindex: 进程标识
    :param sendtopic: Kafka 异常的topic
    :param trajclusters: 聚类簇
    :param cf: 配置文件类
    :return:
    """
    with lock:
        send_producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        fileConfig('Config/logging.conf')
        logger = logging.getLogger('debugLogger')
        logger.debug('debug')
        logger_warning = logging.getLogger('warninglogger')
        logger_warning.warning('warning')

    print("异常生产者连接Kafka成功！")
    while True:
        with lock:
            if not queue.empty():
                job = queue.get(block=False)
                print("work=%d queue length==========>%d<=========== job=" % (workindex, queue.qsize()))
                print(job)
                detect_procss(job, send_producer, sendtopic, trajclusters, cf, logger, logger_warning)

            else:
                print("Nothing to do! work=%d" % workindex)
                time.sleep(1)


def detect_procss(trajpt, send_producer, send_topic, trajclusters, cf, logger, logger_warning):
    """
    首先获取船舶静态信息，然后根据US来查找正在进行检测的船舶，如果没有则创建该船舶检测的实例，如果有则添加该点。如果收到消失报，则从dict中剔除
    :param trajpt: 航迹信息
    :param send_producer: Kafka地址
    :param send_topic: 告警接收的topic
    :param trajclusters: 历史航迹聚类结果dict{1：ClusterDetector}
    :param cf: 配置文件类
    :param logger: debug 日志
    :param logger_warning: warning日志
    :return:
    """
    # Todo if trajpt['Miss']==1:
    #     #如果收到消失报则从船舶dict中剔除
    #     shipDetecting.pop(trajpt['US'])
    #     return

    ship: TraclusPoint.TraclusPoint = shipDetecting.get(trajpt['US'])
    if ship is None:
        # Todo 获取静态信息
        ship_info = TraclusPoint.ShipStaticInformation(trajpt['US'], 100, 10, 5, 1)
        # 非正常抛锚异常队列长度
        len_abnormal_anchor = cf.getint('Detection', 'lenAbnormalAnchor')
        # 速度异常队列长度
        len_abnormal_velocity = cf.getint('Detection', 'lenAbnormalVelocity')
        # 加速度异常队列长度
        len_abnormal_acceleration = cf.getint('Detection', 'lenAbnormalAcceleration')
        # 航向加速度异常队列长度
        len_heading_acceleration = cf.getint('Detection', 'lenHeadingAcceleration')
        # 漂航异常队列长度
        len_abnormal_drifting = cf.getint('Detection', 'lenAbnormalDrifting')
        # 偏离航线异常队列长度
        len_yaw = cf.getint('Detection', 'lenYaw')
        # 仿冒它船队列长度
        len_cheat = cf.getint('Detection', 'lenCheat')
        ship_tp = TraclusPoint.TraclusPoint(static_info=ship_info,
                                            send_producer=send_producer,
                                            send_topic=send_topic,
                                            len_abnormal_anchor=len_abnormal_anchor,
                                            len_abnormal_velocity=len_abnormal_velocity,
                                            len_abnormal_acceleration=len_abnormal_acceleration,
                                            len_heading_acceleration=len_heading_acceleration,
                                            len_abnormal_drifting=len_abnormal_drifting,
                                            len_yaw=len_yaw,
                                            len_cheat=len_cheat)
        shipDetecting[trajpt['US']] = ship_tp
    else:
        ship.set_current_traj_pt(trajpt)
        # 测试这里使用客船训练的簇，实际中应该根据船舶类型选择对应的簇进行搜索
        msg = ship.yaw(trajclusters)
        if msg['type'] != 'None':
            logger_warning.warning(str(msg))
        else:
            logger.debug(str(msg))


def track_detect(config_path):
    """
    航迹检测
    :param config_path:配置文件
    :return:
    """
    if config_path is None:
        config_path = 'Config/detector_config.conf'
    cf = configparser.ConfigParser()
    cf.read(config_path, encoding='UTF-8')
    host_addr = cf.get('Kafka', 'addr')
    recv_topic = cf.get('Kafka', 'recv_topic')
    send_topic = cf.get('Kafka', 'send_topic')
    detectprocessnum = cf.getint('Detection', 'maxDetectionThreadNum')
    # Todo 初始化所有簇
    traj_clusters = TraclusPoint.init_ship_clusters()
    address = host_addr.split(';')
    q = Queue()
    lock = Lock()
    pw = Process(target=recv_process, args=(q, recv_topic, address))
    pw.start()
    for i in range(detectprocessnum):
        pr = Process(target=read_queue, args=(q, lock, i, send_topic, traj_clusters, cf))
        pr.start()


@click.command()
@click.option(
    '--config-path', '-p',
    help='配置文件路径',
    required=True)
def main(config_path):
    track_detect(config_path)

    # address = host_addr.split()
    #
    # consumer = KafkaConsumer(topic, bootstrap_servers=address, auto_offset_reset='latest')

    # obCD = ClusterDetector('E:/SRC/TAD/DataCleaning/SHDATA/0423_Out_Clusters_2.csv', 4)
    # e = []
    # for msg in consumer:
    #     dict = eval(msg.value)
    #     fClusterType = obCD.DetectCluster_UTM(dict['LON'], dict['LAT'], 1000)
    #     if fClusterType == -1:
    #         e.append(dict)
    #         if len(e) > 1000:
    #             with open("Data/error-1.txt", 'w') as output:
    #                 output.write(json.dumps(e))
    #             break
    #     print('经度=%f  纬度=%f 属于第=======>%d<=======个簇' % (dict['LON'], dict['LAT'], fClusterType))


if __name__ == '__main__':
    main()
