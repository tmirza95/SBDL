import configparser
from pyspark.conf import SparkConf

def get_config(env):
    config = configparser.ConfigParser()
    config.read('conf/sbdl.conf')
    dict = {}

    for key,value in config.items(env):
        dict[key] = value
    return dict

def get_spark_conf(env):

    spark_conf = SparkConf()
    config = configparser.ConfigParser()

    config.read('conf/spark.conf')

    for key,value in config.items(env):
        spark_conf.set(key,value)
    return spark_conf


def get_data_filter(env, data_filter):
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]