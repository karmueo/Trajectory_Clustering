# @File  : velocity_clustering.py
# @Author: 沈昌力
# @Date  : 2018/4/25
# @Desc  : 从位置聚类的结果中再次按航向进行聚类。输入为正常聚类以及二次聚类的结果，可以是多个文件，输出为Clusters_angle_reclustering.txt，合并了前面几个文件的聚类结果。
import json
import click
from sklearn.cluster import KMeans      #引入kmeans
import pandas as pd
from plot_train_res import plot_one_cluster
import matplotlib.pyplot as plt
import numpy as np

n_centers = 2  #定义Kmeans聚类的中心个数

def one_cluster_angle_clustering(cluster, show=False):
    """
    输入一个簇，统计该簇的航向直方图，使用Kmeans聚类，把该簇根据航向分为两个簇
    :param cluster: dbscan聚类后得到的簇
    :param show: 是否显示航向直方图
    :return: DataFrame类型的簇，簇中index表示属于新簇的类标识
    """
    df = pd.DataFrame(cluster, columns=['start', 'end', 'angle'])
    km = KMeans(n_clusters=n_centers, random_state=0)
    km.fit(df['angle'].as_matrix().reshape(-1, 1))
    labels = km.labels_
    df = pd.DataFrame(cluster, index=labels, columns=['start', 'end', 'angle'])

    if show == True:
        s = df['angle']
        s.hist(bins=100, alpha=0.3, color='k', normed=True)
        s.plot(kind='kde', style='k--')
        plt.show()
        print('绘制聚类结果图，该簇共有%d条线段' % (df.shape[0]))
        plot_one_cluster(df, n_centers)

    return df


@click.command()
@click.option(
    '--clusters-input-file', '-i',
    help='输入的train_from_cleandata训练得到的航迹聚类文件',
    required=True)
@click.option(
    '--show-clusters', '-s',
    help='是否显示每一簇的结果图', required=False)
def angle_clustering(clusters_input_file, show_clusters=None):
    """
    根据航向进行聚类
    :param clusters_input_file:dbscan聚类后得到的簇文件
    :param show_clusters: 是否显示聚类结果
    """
    files = clusters_input_file.split(',')
    clusters_input = []
    for f in files:
        with open(f, 'r') as clusters_stream:
            c_input = json.loads(clusters_stream.read())
            if clusters_input:
                clusters_input.extend(c_input)
            else:
                clusters_input = c_input

    new_clusters = []
    print('开始航向聚类计算...')
    count = 0
    for c in clusters_input:
        count = count + 1
        if show_clusters!=None:
            angle_clusters = one_cluster_angle_clustering(c, True)
        else:
            angle_clusters = one_cluster_angle_clustering(c)
        print('完成第 %d 个簇的航向聚类' % (count))
        for idx in range(n_centers):
            #航向聚类
            one_angle_cluster : pd.DataFrame = angle_clusters.ix[idx]
            # #计算该类的航向均值好标准差
            # angle_mean = one_angle_cluster['angle'].mean()
            # angle_std = one_angle_cluster['angle'].std()

            d = one_angle_cluster.to_dict(orient='records')
            list_one_angle_cluster = d
            new_clusters.append(list_one_angle_cluster)
    print('航向聚类计算结束...')

    angle_reclustering_file = 'Data/Clusters_angle_reclustering.txt'
    with open(angle_reclustering_file, 'w') as output:
        output.write(json.dumps(new_clusters))

if __name__ == '__main__':
    angle_clustering()