# @File  : train_from_cleandata.py
# @Author: 沈昌力
# @Date  : 2018/4/9
# @Desc  : pre_process.py处理后的json文件作为-i的输入文件，输出为-c指定的文件，可以设置领域范围-e，邻域线段数-n，是否显示聚类结果-p
import click
from traclus_impl.geometry import Point
import json
from traclus_impl.coordination import train_traclus
import matplotlib.pyplot as plt
from plot_train_res import plot_histogram, plot_one_cluster, plot_raw2

@click.command()
@click.option(
    '--input-file', '-i',
    help='输入的航迹json文件，如果是cvs文件需要通过pre_process.py转为json文件再训练',
    required=True)
@click.option(
    '--clusters-output-file-name', '-c',
    help='输出的聚类结果', required=True)
@click.option(
    '--epsilon', '-e',
    help='领域范围', required=False)
@click.option(
    '--min-neighbors', '-n',
    help='邻域线段数', required=False)
@click.option(
    '--show-clusters-angle-histogram', '-a',
    help='是否显示每一簇的航向直方图', required=False)
@click.option(
    '--plot-clusters', '-p',
    help='是否显示每一簇的结果图', required=False)
def main(input_file,
         clusters_output_file_name,
         epsilon=None,
         min_neighbors=None,
         show_clusters_angle_histogram=None,
         plot_clusters=None):

    if plot_clusters==None:
        result = parse_input_and_run_traclus(input_file,
                                         clusters_output_file_name,
                                         epsilon,
                                         min_neighbors,
                                         show_clusters_angle_histogram)
    else:
        result = parse_input_and_run_traclus(input_file,
                                             clusters_output_file_name,
                                             epsilon,
                                             min_neighbors,
                                             show_clusters_angle_histogram,
                                             show_clusters=True)
    print("训练结束")


def parse_input_and_run_traclus(input_file,
                                clusters_output_file_name,
                                epsilon=None,
                                min_neighbors=None,
                                show_clusters_angle_histogram=None,
                                show_clusters=False):
    """
    对输入文件数据进行聚类，并输出聚类结果
    :param input_file: 输入文件
    :param clusters_output_file_name: 输出的簇文件
    :param epsilon: dbscan领域范围
    :param min_neighbors: dbscan领域最小线段数
    :param show_clusters_angle_histogram: 是否显示航向直方图
    :param show_clusters: 是否显示聚类结果图
    :return:
    """
    """
        对输入文件数据进行聚类，并输出聚类结果
        :param input_file: 输入文件
        :param clusters_output_file_name: 输出的簇文件
        :param epsilon: dbscan领域范围
        :param min_neighbors: dbscan领域最小线段数
        :return:
        """
    parsed_input = None
    with open(get_correct_path_to_file(input_file), 'r') as input_stream:
        parsed_input = json.loads(input_stream.read())

    trajs = list(map(lambda traj: list(map(lambda pt: Point(**pt), traj)), parsed_input))
    if show_clusters == True:
        fig_raw = plt.figure('原始航迹')
        plot_raw2(fig_raw, trajs)

    clusters_hook = get_dump_clusters_hook(clusters_output_file_name, show_clusters_angle_histogram, show_clusters=show_clusters)
    print("start run_traclus")

    if epsilon == None:
        epsilon = 2500
    else:
        try:
            epsilon = float(epsilon)
        except:
            print("epsilon 输入的不是数字！用默认值0.08代替了")
            epsilon = 2500

    if min_neighbors == None or min_neighbors.isdigit() == False:
        min_neighbors = 3
    else:
        try:
            min_neighbors = int(min_neighbors)
        except:
            print("min_neighbors输入的不是数字！用默认值3代替了")
            min_neighbors = 3

    return train_traclus(point_iterable_list=trajs,
                         epsilon=epsilon,
                         min_neighbors=min_neighbors,
                         min_vertical_lines=2,
                         clusters_hook=clusters_hook)


def get_dump_clusters_hook(file_name, show_clusters_angle_histogram=None, min_num_trajectories_in_cluster=10, show_clusters=False):
    if not file_name:
        return None

    def func(clusters, noises):
        mian_cluster_line_segs = []
        small_cluster_line_segs = []
        num_clusters = len(clusters)
        print("一共有%d个簇" % (num_clusters))
        index = 1

        for clust in clusters:
            if show_clusters_angle_histogram!=None:
                #计算角度直方图
                angles = clust.angle_histogram()
                fig = plt.figure(str(index))
                plot_histogram(fig, angles)

            #统计大簇和小簇
            line_segs = clust.get_trajectory_line_segments()
            dict_output = list(map(lambda traj_line_seg: traj_line_seg.line_segment.as_dict(),
                                   line_segs))

            if show_clusters == True:
                title = '该簇共有' + str(clust.num_trajectories_contained()) + '条线段'
                fig_cluster = plt.figure(title)
                print('开始绘制第%d个簇' % (index))
                #随机取一个颜色作为簇的颜色
                import numpy as np
                color = np.random.randint(16, 255, size=3)
                co = list(map(lambda c: c[2:].upper(), list(map(hex, color))))
                str_corlor = '#' + co[0] + co[1] + co[2]
                #画簇
                plot_one_cluster(fig_cluster, dict_output, str_corlor)
                print('绘制第%d个簇完成' % (index))

            if clust.num_trajectories_contained() < min_num_trajectories_in_cluster:
                small_cluster_line_segs.append(dict_output)
            else:
                mian_cluster_line_segs.append(dict_output)
            index = index + 1

        noise_cluster_line_segs = list(map(lambda traj_line_seg: traj_line_seg.line_segment.as_dict(), noises))

        print("有%d个主簇" % (len(mian_cluster_line_segs)))
        print("剩下%d个簇认为是小簇" % (len(small_cluster_line_segs)))
        with open(get_correct_path_to_file(file_name), 'w') as output:
            output.write(json.dumps(mian_cluster_line_segs))

        tmp_str = file_name.split('.')
        assert len(tmp_str) == 2
        small_file = tmp_str[0] + '_small.txt'
        with open(get_correct_path_to_file(small_file), 'w') as output:
            output.write(json.dumps(small_cluster_line_segs))

        print("有%d条轨迹线段为噪声" % (len(noise_cluster_line_segs)))
        noise_file = tmp_str[0] + '_noise.txt'
        with open(get_correct_path_to_file(noise_file), 'w') as output:
            output.write(json.dumps(noise_cluster_line_segs))

        if show_clusters_angle_histogram!=None or show_clusters==True:
            plt.show()
    return func


def get_correct_path_to_file(file_name):
    return file_name


if __name__ == '__main__':
    main()
