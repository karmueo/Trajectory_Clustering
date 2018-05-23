# @File  : track_detection.py
# @Author: 沈昌力
# @Date  : 2018/4/2
# @Desc  : 将csv航迹转换为json格式
import pandas as pd
import json
import click
import time


def mycomp(us, data):
    """
    重排
    :param us: 唯一标识Series
    :param data: 原始数据
    :return: 重新排序一个us对应该us所有的航迹点的信息
    """
    trajs = []
    for u in us:
        u_data = data[data['US'] == u]
        u_data: pd.DataFrame = u_data[['Y', 'X', 'C', 'V', 'TIME']]
        u_data.rename(columns={'Y': 'x', 'X': 'y'}, inplace=True)
        traj = u_data.to_dict('records')
        trajs.append(traj)
    return trajs


@click.command()
@click.option('--input-file', '-i', required=True)
def rawcsv2json(input_file):
    """
    将DataCleaning抽稀后的csv文件转为json格式的文件
    :param input_file:csv文件
    :return:
    """
    data = pd.read_csv(input_file)
    data_ = data[data['C'] != -1]
    us = data_['US']
    us = us.drop_duplicates()
    print("开始转换")
    start = time.time()

    trajs = mycomp(us, data_)

    tmp_str = input_file.split('.')
    assert len(tmp_str) == 2
    output_file = tmp_str[0] + '_json_out.txt'
    with open(output_file, 'w') as output:
        str_trajs = json.dumps(trajs)
        output.write(str_trajs)
    print("转换结束")
    end = time.time()
    print("------------>")
    print(end - start)
    print("<------------")


if __name__ == '__main__':
    rawcsv2json()
