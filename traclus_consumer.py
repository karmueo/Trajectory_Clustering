# @File  : traclus_consumer.py
# @Author: 沈昌力
# @Date  : 2018/4/17
# @Desc  :
from kafka import KafkaConsumer
import click
from ClusterDetector import ClusterDetector
import json

@click.command()
@click.option(
    '--host_addr', '-h',
    help='kafka服务器地址',
    required=True)
@click.option(
    '--topic', '-t',
    help='topic名称',
    required=True)
def main(host_addr, topic):
    address = host_addr.split()

    consumer = KafkaConsumer(topic, bootstrap_servers=address, auto_offset_reset='latest')

    obCD = ClusterDetector('E:/SRC/TAD/DataCleaning/SHDATA/0423_Out_Clusters_2.csv', 4)
    # count = obCD.GetClusterTypeCount()
    # c = []
    e = []
    # for i in range(count):
    #     p = []
    #     c.append(p)
    for msg in consumer:
        dict = eval(msg.value)
        fClusterType = obCD.DetectCluster_UTM(dict['LON'], dict['LAT'], 1000)
        if fClusterType == -1:
            e.append(dict)
            if len(e) > 1000:
                with open("Data/error-1.txt", 'w') as output:
                    output.write(json.dumps(e))
                break
        # else:
        #     c[fClusterType].append(msg.value)
        print('经度=%f  纬度=%f 属于第=======>%d<=======个簇' % (dict['LON'], dict['LAT'], fClusterType))
        # fClusterType = obCD.DetectCluster(121.36612132, 31.538823)
    # for i in range(count):
    #     print('第%d簇共有%d个航迹点' % (i+1, len(c[i])))
    # print('共有%d个航迹点没有找到簇' % (len(e[i])))

if __name__ == '__main__':
    main()