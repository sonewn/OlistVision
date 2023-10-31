from pyspark.sql import SparkSession
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os

spark = SparkSession.builder.master("local[*]").getOrCreate()

## Load Source Data
olist_geo = spark.read.csv('../bucket/olist_source/olist_geolocation_dataset.csv',inferSchema=True,header=True)
olist_geo.createOrReplaceTempView('olist_geolocation')

def main():
    """
    Preprocessing zipcode data for the overlap issue of zip codes by state and country
    """

    qr = """
    SELECT * FROM olist_geolocation
    """
    zipdis = spark.sql(qr).toPandas()
    print(zipdis)

    # 건수가 많은 도시로 코드 별 도시 재정의
    zipchk = zipdis.groupby(['geolocation_zip_code_prefix','geolocation_city']).count().reset_index()
    chklst = zipchk.geolocation_zip_code_prefix.unique()

    df = pd.DataFrame()
    for c in chklst:
        add = zipchk[zipchk.geolocation_zip_code_prefix == c].head(1)
        df = pd.concat([df,add])

    # 건수가 많은 도시 이름의 좌표만 코드마다 배당
    res = pd.DataFrame()
    for cd, city in zip(df.geolocation_zip_code_prefix, df.geolocation_city):
        add = zipdis[(zipdis.geolocation_zip_code_prefix == cd) & (zipdis.geolocation_city == city)]
        res = pd.concat([res, add])

    res.to_csv('../bucket/olist_geocode_prepro.csv', index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    main()
