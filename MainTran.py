# @File  : MainTran.py
# @Author: 沈昌力
# @Date  : 2018/5/10
# @Desc  : 1、DataCleaning预处理，抽稀，停泊点聚类等；2、train_from_cleandata进行第一次dbscan航迹聚类；3、secondary_train二次聚类；4、angle_reclustering航向聚类
import click
import configparser
import train_from_cleandata
import secondary_train
import angle_reclustering
import plot_train_res


@click.command()
@click.option(
    '--config-path', '-p',
    help='配置文件路径',
    required=True)
def main(config_path):
    if config_path is None:
        config_path = 'Config/train_config.conf'
    cf = configparser.ConfigParser()
    cf.read(config_path, encoding='UTF-8')
    train_from_cleandata.first_train(cf)
    print("一次训练结束")

    secondary_train.secondary_train(cf)
    print("二次训练结束")

    angle_reclustering.angle_clustering(cf)
    print("航向聚类结束")

    is_show_res = cf.getboolean('plot_res', 'show_res')
    if is_show_res:
        plot_train_res.plot_res(cf)


if __name__ == '__main__':
    main()
