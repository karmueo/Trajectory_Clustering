# @File  : velocity_clustering.py
# @Author: 沈昌力
# @Date  : 2018/5/23
# @Desc  :
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def one_cluster_velocity_clustering(cluster, show=False):
    v = [(traj['start']['v']+traj['end']['v'])/2. for traj in cluster]

    df = pd.DataFrame(v)
    p = df.boxplot(return_type='dict')
    bottom = p['whiskers'][0].get_ydata()[1]
    top = p['whiskers'][1].get_ydata()[1]
    if show is True:
        plt.show()
    return bottom, top


def velocity_clustering():
    """
    根据速度进行聚类
    :param config: 配置文件
    :return:
    """
    clusters_input_file = 'Data/Clusters_angle_reclustering.txt'
    with open(clusters_input_file, 'r') as clusters_stream:
        c_input = json.loads(clusters_stream.read())

    for c in c_input:
        bottom, top = one_cluster_velocity_clustering(c, True)
        print(bottom)

if __name__ == '__main__':
    velocity_clustering()
