# @File  : plot_error-1.py
# @Author: 沈昌力
# @Date  : 2018/4/24
# @Desc  :
import click
import json
import matplotlib.pyplot as plt
import UTMConvertor
import numpy as np


def plot_raw(trajectorys):
    x = []
    y = []
    for traj in trajectorys:
        x.clear()
        y.clear()
        for pt in traj:
            x.append(pt['x'])
            y.append(pt['y'])
        plt.plot(y, x, 'red')

def plot_error_points(points):
    utmAccu = 1000
    x = []
    y = []
    for p in points:
        xMeter, yMeter = UTMConvertor.LonLat2Mercator_One(p['LON'], p['LAT'])
        cX = int(xMeter / utmAccu) * utmAccu
        cY = int(yMeter / utmAccu) * utmAccu
        x.append(cX)
        y.append(cY)
    plt.scatter(x, y, c='blue', marker='.')
        # print(p)

@click.command()
@click.option(
    '--input-file', '-i',
    help='原始航迹文件',
    required=True)
@click.option(
    '--error-file-name', '-e',
    help='聚类结果文件',
    required=True)
def main(input_file, error_file_name):
    fig1 = plt.figure('航迹')
    print("==========开始绘制原始航迹图...==========")
    with open(input_file, 'r') as trajectorys_stream:
        trajectorys = json.loads(trajectorys_stream.read())
        plot_raw(trajectorys)
    print("==========绘制原始航迹图完成==========\n")

    with open(error_file_name, 'r') as error_stream:
        error_points = json.loads(error_stream.read())
        plot_error_points(error_points)
    plt.show()


if __name__ == '__main__':
    main()