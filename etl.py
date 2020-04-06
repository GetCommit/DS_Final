# Imports
import sys
from os import path
from pyspark.sql import SparkSession
from cache import *

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import isnan, when, count, col
import json

def create_dataframe(filepath, format, spark):
    """
    Create a spark df given a filepath and format.
    :param filepath: <str>, the filepath
    :param format: <str>, the file format (e.g. "csv" or "json")
    :param spark: <str> the spark session
    :return: the spark df uploaded
    """
    spark_df = spark.read.format(format).options(header='true', inferSchema='true').load(filepath)
    return spark_df

def geo_tranform(latitude, longitude):
    if latitude is None:
        return None
    result = get_neighborhood(latitude, longitude)
    if result is None:
        return result
    return result.lower()


def transform_restaurant_data(df):
    """
    Transform geo info from latitude/ longitude to NYC NEIGHBORHOOD
    :param df: spark df
    :return: spark df, transformed df
    """
    transformed_df = None
    geo_trans = F.udf(geo_tranform, StringType())
    transformed_df = df.withColumn("GEO_TRANS", geo_trans(df.Latitude, df.Longitude))
    return transformed_df



def neighbor_tranform(neighbor):
    if neighbor is None:
        return neighbor
    # delete the relative location info from neighbor
    return neighbor.split('-')[0].lower()

def transform_property_data(df):
    """
        Transform geo info from latitude/ longitude to NYC NEIGHBORHOOD
        :param df: spark df
        :return: spark df, transformed df
    """
    transformed_df = None
    neighbor_trans = F.udf(neighbor_tranform, StringType())
    transformed_df = df.withColumn("NEIGHBOR_TRANS", neighbor_trans(df.NEIGHBORHOOD))
    return transformed_df


if __name__ == '__main__':
    # Start spark session
    spark = SparkSession.builder.getOrCreate()

    restaurant_file_arg = "DOHMH_New_York_City_Restaurant_Inspection_Results.csv"
    restaurant_file_type = restaurant_file_arg.split('.')[-1]
    if not path.exists("DOHMH_New_York_City_Restaurant_Inspection_Results.csv"):
        print("Restaurant data not found.")

    property_file_arg = "DOF__Summary_of_Neighborhood_Sales_by_Neighborhood_Citywide_by_Borough.csv"
    property_file_type = property_file_arg.split('.')[-1]
    if not path.exists("DOF__Summary_of_Neighborhood_Sales_by_Neighborhood_Citywide_by_Borough.csv"):
        print("Property sales data not found.")

    # load both dataframes
    restaurant_df = create_dataframe(restaurant_file_arg, restaurant_file_type, spark)
    property_df = create_dataframe(property_file_arg, property_file_type, spark)

    # transform geo info to neighbor info in restaurant df
    restaurant_df = transform_restaurant_data(restaurant_df)
    # restaurant_df.show()
    property_df = transform_property_data(property_df)
    # property_df.show()

    restaurant_df = restaurant_df.na.drop(subset=["GEO_TRANS"])
    property_df = property_df.na.drop(subset=["NEIGHBOR_TRANS"])

    # Test number of nulls in df
    # restaurant_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in restaurant_df.columns]).show()
    # print(restaurant_df.count())

    # print(restaurant_df)
    df = restaurant_df.join(property_df, (restaurant_df.GEO_TRANS.contains(property_df.NEIGHBOR_TRANS)) | (restaurant_df.GEO_TRANS == property_df.NEIGHBOR_TRANS), how='inner')
    # df.show()
    print(df.count())
    df.coalesce(1).write.csv("Joined Data", header='true')
    spark.stop()
