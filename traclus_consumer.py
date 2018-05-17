# @File  : traclus_consumer.py
# @Author: 沈昌力
# @Date  : 2018/4/17
# @Desc  : 实时航迹检测，kafka消费者，-h输入kafuka服务器地址：-h 127.0.0.1:9092，-t输入topic名称：-t test2
from kafka import KafkaConsumer
import click
import configparser
from ClusterDetector import ClusterDetector
import json
import time
from multiprocessing import Process, Queue, Lock
import TraclusPoint

shipDetecting = {}  #所有当前正在监控的船舶

def RecvProcess(queue, topic, address):
    """
    从Kafka接收数据，放入队列
    :param queue: 队列
    :param topic: Kafka的Topic
    :param address: Kafka的地址
    :return:
    """
    consumer = KafkaConsumer(topic, bootstrap_servers=address, auto_offset_reset='largest')
    print("consumer连接Kafka成功")
    while True:
        for msg in consumer:
            print("recving thread. current queue length = %d" % (queue.qsize()))
            dict = eval(msg.value)
            queue.put(dict)  # 放入到任务队列中去
            # time.sleep(1)

def ReadQueue(queue, lock, workindex, address, sendtopic):
    """
    从队列中读取数据，进行航迹检测
    :param queue: 队列
    :param lock: 进程锁
    :param workindex: 进程标识
    :return:
    """
    while True:
        with lock:
            if not queue.empty():
                job = queue.get(block=False)
                print("work=%d queue length==========>%d<=========== job=" %(workindex, queue.qsize()))
                print(job)
                DetectProcss(job, address, sendtopic)

            else:
                print("Nothing to do! work=%d" %(workindex))
                time.sleep(1)

def DetectProcss(trajpt, address, send_topic):
    """
    首先获取船舶静态信息，然后根据US来查找正在进行检测的船舶，如果没有则创建该船舶检测的实例，如果有则添加该点。如果收到消失报，则从dict中剔除
    :param trajpt: 航迹信息
    :param address: Kafka地址
    :param send_topic: 告警接收的topic
    :return:
    """
    if trajpt['Miss']==1:
        #如果收到消失报则从船舶dict中剔除
        shipDetecting.pop(trajpt['US'])
        return

    #获取静态信息
    shipInfo= TraclusPoint.ShipStaticInformation(trajpt['US'], 100, 10, 5, 1, None)
    ship:TraclusPoint.TraclusPoint = shipDetecting.get(trajpt['US'])
    if ship == None:
        ship_tp = TraclusPoint.TraclusPoint(shipInfo, address, send_topic)
        shipDetecting[trajpt['US']] = ship_tp
    else:
        ship.SetCurrentTrajPt(trajpt)

def TrackDetect(host_addr, recv_topic, send_topic, detectprocessnum):
    """
    航迹检测
    :param host_addr:Kafka地址
    :param recv_topic: Kafka的Topic
    :param detectprocessnum: 检测进程的个数
    :return:
    """
    address = host_addr.split(';')
    q = Queue()
    lock = Lock()
    pw = Process(target=RecvProcess, args=(q, recv_topic, address))
    pw.start()
    for i in range(detectprocessnum):
        pr = Process(target=ReadQueue, args=(q, lock, i, address, send_topic))
        pr.start()





@click.command()
@click.option(
    '--config-path', '-p',
    help='配置文件路径',
    required=True)
def main(config_path):
    if config_path == None:
        config_path = 'Config/detector_config.conf'
    cf = configparser.ConfigParser()
    cf.read(config_path, encoding='UTF-8')
    address = cf.get('Kafka', 'addr')
    recvtopic = cf.get('Kafka', 'recv_topic')
    sendtopic = cf.get('Kafka', 'send_topic')
    maxDetectionThreadNum = cf.getint('Detection', 'maxDetectionThreadNum')
    TrackDetect(address, recvtopic, sendtopic, maxDetectionThreadNum)

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