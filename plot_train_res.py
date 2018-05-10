# @File  : plot_train_res.py
# @Author: 沈昌力
# @Date  : 2018/4/9
# @Desc  : 绘制训练结果图
import json
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import click
import pandas as pd

def plot_one_cluster(fig, cluster, str_corlor):
    """
    绘制一个簇的航迹
    :param fig: 指定绘制的图
    :param cluster: 需要绘制的簇信息
    :param str_corlor: 绘制的颜色
    :return: None
    """
    # 指定默认字体
    matplotlib.rcParams['font.sans-serif'] = ['SimHei']
    matplotlib.rcParams['font.family'] = 'sans-serif'
    # 解决负号'-'显示为方块的问题
    matplotlib.rcParams['axes.unicode_minus'] = False
    x = []
    y = []
    ax = fig.add_subplot(111)
    for line in cluster:
        start = line['start']
        end = line['end']
        x.clear()
        y.clear()
        x.append(start['x'])
        y.append(start['y'])
        x.append(end['x'])
        y.append(end['y'])
        ax.plot(y, x, c=str_corlor)  # 调用plot在当前的figure对象中绘图实际
    # plt.axis([121, 123, 30.8, 32])
    plt.xlabel('经度')
    plt.ylabel('纬度')


def plot_one_cluster(cluster:pd.DataFrame, centers):
    """
    绘制一个簇的航迹
    :param cluster: 需要绘制的簇信息
    :param centers:
    :return:
    """
    for idx in range(centers):
        color = np.random.randint(16, 255, size=3)
        co = list(map(lambda c: c[2:].upper(), list(map(hex, color))))
        str_corlor = '#' + co[0] + co[1] + co[2]
        df:pd.DataFrame = cluster.ix[idx]
        num = df.shape[0]
        n_start = df.as_matrix(columns=['start']).tolist()
        n_end = df.as_matrix(columns=['end']).tolist()
        for i in range(num):
            sx = n_start[i][0]['x']
            sy = n_start[i][0]['y']
            ex = n_end[i][0]['x']
            ey = n_end[i][0]['y']
            plt.plot([sy, ey], [sx, ex], c=str_corlor)  # 调用plot在当前的figure对象中绘图实际
    plt.show()

def plot_clusters(pic, fig1, clusters, title):
    """
    绘制簇图谱
    :param pic: 子图
    :param fig1: 主图
    :param clusters: 簇信息
    :param title: 图标题
    :return:
    """
    x = []
    y = []

    print('一共有%d个簇' % (len(clusters)))

    for c in clusters:
        v = np.zeros(len(c)*2)
        v_index = 0
        color = np.random.randint(16, 255, size=3)
        co = list(map(lambda c: c[2:].upper(), list(map(hex, color))))
        str_corlor = '#' + co[0] + co[1] + co[2]
        for pt in c:
            start = pt['start']
            end = pt['end']
            v[v_index] = start['v']
            v[v_index+1] = end['v']
            v_index = v_index + 2
            x.clear()
            y.clear()
            x.append(start['x'])
            y.append(start['y'])
            x.append(end['x'])
            y.append(end['y'])
            if pic == None:
                fig1.plot(y, x, c=str_corlor)  # 调用plot在当前的figure对象中绘图实际
                plt.title(title)
                # plt.axis([121, 123, 30.8, 32])
            else:
                pic.plot(y, x, c=str_corlor)  # 调用plot在当前的figure对象中绘图实际
                plt.title(title)
                # plt.axis([121, 123, 30.8, 32])



def plot_noise(pic, noise):
    """
    绘制噪声图
    :param pic:指定绘制的图
    :param noise: 噪声信息
    :return:
    """
    x = []
    y = []
    print('一共有%d条噪声线段' % (len(noise)))
    for pt in noise:
        start = pt['start']
        end = pt['end']
        x.clear()
        y.clear()
        x.append(start['x'])
        y.append(start['y'])
        x.append(end['x'])
        y.append(end['y'])
        if pic == None:
            plt.plot(y, x)  # 调用plot在当前的figure对象中绘图实际
            plt.title('noise')
            # plt.axis([121, 123, 30.8, 32])
        else:
            pic.plot(y, x)  # 调用plot在当前的figure对象中绘图实际
            plt.title('noise')
            # plt.axis([121, 123, 30.8, 32])


def plot_raw2(fig, trajectorys):
    """
    绘制原始航迹
    :param fig: 子图
    :param trajectorys: 原始航迹
    :return:
    """
    print('开始绘制原始航迹图...')
    # 指定默认字体
    matplotlib.rcParams['font.sans-serif'] = ['SimHei']
    matplotlib.rcParams['font.family'] = 'sans-serif'
    # 解决负号'-'显示为方块的问题
    matplotlib.rcParams['axes.unicode_minus'] = False
    x = []
    y = []
    ax = fig.add_subplot(111)
    for traj in trajectorys:
        x.clear()
        y.clear()
        for pt in traj:
            x.append(pt.x)
            y.append(pt.y)
        ax.plot(y, x)
    # plt.axis([121, 123, 30.8, 32])
    plt.xlabel('经度')
    plt.ylabel('纬度')
    print('绘制原始航迹图完成')

def plot_raw(pic, trajectorys):
    """
    绘制原始航迹
    :param pic:绘制的图
    :param trajectorys:航迹点信息
    :return:
    """
    x = []
    y = []
    for traj in trajectorys:
        x.clear()
        y.clear()
        for pt in traj:
            x.append(pt['x'])
            y.append(pt['y'])
        pic.plot(y, x)
        plt.title('raw')
        # plt.axis([121, 123, 30.8, 32])


def plot_histogram(fig, hist):
    """
    绘制直方图
    :param fig: 子图
    :param hist: 直方图数据
    :return:
    """
    # 指定默认字体
    matplotlib.rcParams['font.sans-serif'] = ['SimHei']
    matplotlib.rcParams['font.family'] = 'sans-serif'
    # 解决负号'-'显示为方块的问题
    matplotlib.rcParams['axes.unicode_minus'] = False

    ax = fig.add_subplot(111)
    ax.hist(hist, bins=200)
    ax.set_title('该簇一共有%d个点' % (len(hist)))
    plt.xlabel('航向')
    plt.ylabel('航迹点数')


@click.command()
@click.option(
    '--input-file', '-i',
    help='原始航迹文件',
    required=True)
@click.option(
    '--clusters-output-file-name', '-c',
    help='聚类结果文件',
    required=True)
def main(input_file, clusters_output_file_name):
    fig1 = plt.figure('航迹')
    # fig2 = plt.figure('速度分布')
    print("==========开始绘制原始航迹图...==========")
    with open(input_file, 'r') as trajectorys_stream:
        trajectorys = json.loads(trajectorys_stream.read())
        a1 = fig1.add_subplot(221)
        plot_raw(a1, trajectorys)
    print("==========绘制原始航迹图完成==========\n")

    print("==========开始绘制聚类大簇航迹图...==========\n")
    with open(clusters_output_file_name, 'r') as clusters_stream:
        clusters_input = json.loads(clusters_stream.read())
        a2 = fig1.add_subplot(222)
        plot_clusters(a2, fig1, clusters_input, 'main clusters')
    # plt.show()
    print("==========绘制聚类大簇图完成==========\n")

    print("==========开始绘制聚类小簇图...==========\n")
    tmp_str = clusters_output_file_name.split('.')
    assert len(tmp_str) == 2
    small_file = tmp_str[0] + '_small.txt'
    with open(small_file, 'r') as small_stream:
        small_input = json.loads(small_stream.read())
        a3 = fig1.add_subplot(223)
        plot_clusters(a3, fig1, small_input, 'small clusters')
    print("==========绘制聚类小簇图完成==========\n")

    print("==========开始绘制噪声图...==========\n")
    noise_file = tmp_str[0] + '_noise.txt'
    with open(noise_file, 'r') as noise_stream:
        noise_input = json.loads(noise_stream.read())
        a4 = fig1.add_subplot(224)
        plot_noise(a4, fig1, noise_input)
    print("==========绘制噪声图完成==========\n")
    plt.show()


if __name__ == '__main__':
    main()
