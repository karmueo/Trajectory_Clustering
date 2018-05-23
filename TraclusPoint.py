# @File  : TraclusPoint.py
# @Author: 沈昌力
# @Date  : 2018/5/14
# @Desc  : 实时航迹点类
from traclus_impl.geometry import Point
from ClusterDetector import ClusterDetector


def init_ship_clusters():
    traj_clusters = {}
    clusters_1 = ClusterDetector('E:/SRC/TAD/DataCleaning/SHDATA/dotInfo61_UTM_clusters_grid.csv', 4)
    traj_clusters[1] = clusters_1
    return traj_clusters


class ShipStaticInformation:
    """
    船舶静态信息
    """

    def __init__(self, us, legth, width, draught, type):
        self.us = us  # ID
        self.length = legth  # 船长
        self.width = width  # 船宽
        self.draught = draught  # 吃水
        self.type = type  # 船舶类型
        # self.clusters = clusters    #历史位置聚类簇


class TraclusPoint:
    def __init__(self,
                 static_info,
                 send_producer,
                 send_topic,
                 len_abnormal_anchor,
                 len_abnormal_velocity,
                 len_abnormal_acceleration,
                 len_heading_acceleration,
                 len_abnormal_drifting,
                 len_yaw,
                 len_cheat):
        self.shipInf = static_info  # 船舶静态信息
        self._sendTopic = send_topic  # 告警Topic
        self._producer = send_producer  # 异常告警的Kafka生产者
        self._MaxAbnormalAnchor = len_abnormal_anchor  # 非正常抛锚异常队列长度
        self._MaxAbnormalVelocity = len_abnormal_velocity  # 速度异常队列长度
        self._MaxAbnormalAcceleration = len_abnormal_acceleration  # 加速度异常队列长度
        self._MaxHeadingAcceleration = len_heading_acceleration  # 航向加速度异常队列长度
        self._MaxAbnormalDrifting = len_abnormal_drifting  # 漂航异常队列长度
        self._MaxYaw = len_yaw  # 偏离航线异常队列长度
        self._MaxCheat = len_cheat  # 仿冒它船队列长度
        self._acceleration = 0  # 加速度
        self._heading_acceleration = 0  # 航向加速度
        self._len_abnormal_anchor = 0  # 非正常抛锚队列
        self._len_abnormal_velocity = 0  # 异常速度队列
        self._len_abnormal_acceleration = 0  # 异常加速度队列
        self._len_abnormal_heading_acceleration = 0  # 异常航向加速度队列
        self._len_abnormal_drifting = 0  # 疑似漂航队列
        self._len_yaw = 0  # 偏离航向异常队列
        self._len_cheat = 0  # 仿冒它船
        self._warrMsg = {}  # 告警信息
        self._lastPt = Point(0, 0, 0, 0)  # 上一个点
        self._currentPt = Point(0, 0, 0, 0)  # 当前点
        self._acceleration = 0.  # 加速度
        self._heading_acceleration = 0.  # 航向加速度
        self._trajPtDict = {}  # 原始接收的报文字典

    def send2kafka(self, msg):
        """
        发生数据到Kafka队列
        :param msg: dict类型的告警信息
        :return:
        """
        print(msg)
        self._producer.send(self._sendTopic, msg)
        self._producer.flush()

    def set_current_traj_pt(self, pt_dict):
        # v = pt_dict['V']
        # c = pt_dict['C']
        # time = pt_dict['TIME']
        # npLon = np.array(pt_dict['LON'])
        # npLat = np.array(pt_dict['LAT'])
        # lonlatArray = np.transpose(np.vstack((npLon, npLat)))
        # del npLon, npLat
        # xyArray = utm.LonLat2Mercator(lonlatArray)
        # print(xyArray)
        self._trajPtDict = pt_dict
        self._lastPt = self._currentPt  # 上一个点
        self._currentPt = \
            Point(x=pt_dict['LON'], y=pt_dict['LAT'], C=pt_dict['C'], V=pt_dict['V'], TIME=pt_dict['TIME'])  # 当前点
        # self._currentPt = Point(xyArray[0][0], xyArray[0][1], pt_dict['C'], pt_dict['V'])  # 当前点
        self._acceleration = pt_dict['V'] - self._lastPt.v  # 加速度
        self._heading_acceleration = pt_dict['C'] - self._lastPt.c  # 航向加速度

    def abnormal_anchor(self, anchor_clusters):
        """
        计算非正常抛锚判断
        :param anchor_clusters: 常见的停泊位置簇
        :return:
        """
        # 遍历AnchorClusters如果找不到则self._lenAbnormalAnchor+1

        # if self._lenAbnormalAnchor > self._MaxAbnormalAnchor:
        # 告警

    def yaw(self, clusters: ClusterDetector):
        """
        计算偏航
        :param clusters: 其他船舶航迹聚类簇
        :return:
        """
        traj_clusters = clusters[self.shipInf.type]
        if traj_clusters.DetectCluster_UTM(self._currentPt.x, self._currentPt.y, 1000) == -1:
            # 只要没有在自己簇中找到位置，就偏航异常+1
            self._len_yaw = self._len_yaw + 1
            # 只有渔船会仿冒它船，所以仿冒它船只计算渔船的步伐
            if self.shipInf.type == 3:  # 是渔船
                # Todo 1 表示客船，后面还可以加货船等
                if clusters[1].DetectCluster_UTM(self._currentPt.x, self._currentPt.y, 1000) > 0:
                    self._len_cheat = self._len_cheat + 1
        else:
            self._len_yaw = 0
            self._len_cheat = 0

        self._warrMsg['point'] = self._trajPtDict

        if self._len_yaw > self._MaxYaw:
            # 偏航异常告警
            print('偏航异常告警')
            self._warrMsg['type'] = 'yaw'
            self.send2kafka(self._warrMsg)
            return self._warrMsg
        if self._len_cheat > self._MaxCheat:
            # 仿冒它船异常告警
            print('仿冒它船异常告警')
            self._warrMsg['type'] = 'cheat'
            self.send2kafka(self._warrMsg)
            return self._warrMsg

        self._warrMsg['type'] = 'None'
        return self._warrMsg
