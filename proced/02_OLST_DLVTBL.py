from pyspark.sql import SparkSession
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os
from haversine import haversine

spark = SparkSession.builder.master("local[*]").getOrCreate()

## Load Source Data
for dirname, _, filenames in os.walk('../bucket/olist_source'):
    for filename in filenames:
        fn = filename.split('.csv')[0].split('_dataset')[0]
        globals()[fn] = spark.read.csv(os.path.join(dirname, filename),inferSchema=True,header=True)
        globals()[fn].createOrReplaceTempView(f"{fn}")

        print(f"{fn} 's shape is ") #, globals()[fn].shape)

## 1차 가공 DLV_TBL (일자별) + 카테고리 영문명 추가 (23.09.25)
def main():
    qr = f"""
    WITH OTMP1 AS(
    SELECT A.ORDER_ID
            , A.CUSTOMER_ID
            , SPLIT(A.ORDER_PURCHASE_TIMESTAMP, '-')[0]                            AS PUR_YEAR
            , SUBSTR(DATE_FORMAT(TO_DATE( SPLIT(A.ORDER_PURCHASE_TIMESTAMP,' ')[0], 'yyyy-MM-dd'),'yyyyMMdd'),1,6)  AS REFYM
            , TO_DATE( SPLIT(A.order_purchase_timestamp,' ')[0], 'yyyy-MM-dd')             AS order_purchase_timestamp
            , TO_DATE( SPLIT(A.order_approved_at,' ')[0], 'yyyy-MM-dd')                    AS order_approved_at
            , TO_DATE( SPLIT(A.order_delivered_carrier_date,' ')[0], 'yyyy-MM-dd')         AS order_delivered_carrier_date
            , TO_DATE( SPLIT(A.order_delivered_customer_date,' ')[0], 'yyyy-MM-dd')        AS order_delivered_customer_date
            , TO_DATE( SPLIT(A.order_estimated_delivery_date,' ')[0], 'yyyy-MM-dd')        AS order_estimated_delivery_date

            , TO_DATE( SPLIT(B.shipping_limit_date,' ')[0], 'yyyy-MM-dd')                  AS shipping_limit_date
            , B.PRODUCT_ID
            , B.SELLER_ID
            , B.PRICE
            , B.FREIGHT_VALUE
    FROM       OLIST_ORDERS       AS A
    LEFT JOIN  OLIST_ORDER_ITEMS  AS B
    ON         A.ORDER_ID = B.ORDER_ID
    WHERE      A.ORDER_STATUS = 'delivered'
    -- 110197
    )
    , OTMP2 AS (
    SELECT A.ORDER_ID
            , A.CUSTOMER_ID
            , A.PUR_YEAR
            , A.REFYM
            , A.order_purchase_timestamp
            , A.order_approved_at
            , A.order_delivered_carrier_date
            , A.order_delivered_customer_date
            , A.order_estimated_delivery_date
            , A.shipping_limit_date
            , DATEDIFF(A.order_delivered_carrier_date, A.order_approved_at)  CARRIER_DIF                -- 승인 후 발송 DLV
            , DATEDIFF(A.order_delivered_customer_date, A.order_estimated_delivery_date)  ESTIMATE_DIF  -- 기대 예측
            , DATEDIFF(A.order_delivered_carrier_date, A.shipping_limit_date)  RELIABL_DIF              -- 신뢰성
            , DATEDIFF(A.order_delivered_customer_date, A.order_purchase_timestamp)  DLV_TIME            -- 고객 체감 배송시간
            , A.PRODUCT_ID
            , A.SELLER_ID
            , A.PRICE
            , A.FREIGHT_VALUE
            , B.SELLER_CITY
            , B.SELLER_STATE
            , B.SELLER_ZIP_CODE_PREFIX
            , C.PRODUCT_CATEGORY_NAME
            , C.PRODUCT_CATEGORY_NAME_ENGLISH
            , D.CUSTOMER_UNIQUE_ID
            , D.CUSTOMER_ZIP_CODE_PREFIX

    FROM        OTMP1           AS A
    LEFT JOIN   OLIST_SELLERS   AS B
    ON          A.SELLER_ID = B.SELLER_ID
    LEFT JOIN   OLST_PRD_TBL    AS C
    ON          A.PRODUCT_ID = C.PRODUCT_ID
    LEFT JOIN   OLIST_CUSTOMERS AS D
    ON          A.CUSTOMER_ID = D.CUSTOMER_ID
        -- 110197
    )
    , GEO_TBL AS(
    SELECT geolocation_zip_code_prefix, geolocation_city
            , AVG(geolocation_lat) GEOLOCATION_LAT, AVG(geolocation_lng) GEOLOCATION_LNG
    FROM GEO_PREPRO
    GROUP BY 1,2
    -- 19015
    )
    , RPT1 AS (
    SELECT A.ORDER_ID
            , A.CUSTOMER_ID
            , A.PUR_YEAR
            , A.REFYM
            , A.order_purchase_timestamp
            , A.order_approved_at
            , A.order_delivered_carrier_date
            , A.order_delivered_customer_date
            , A.order_estimated_delivery_date
            , A.shipping_limit_date
            , A.CARRIER_DIF                -- 승인 후 발송 DLV
            , A.ESTIMATE_DIF  -- 기대 예측
            , A.RELIABL_DIF              -- 신뢰성
            , A.DLV_TIME
            , A.PRODUCT_ID
            , A.SELLER_ID
            , A.PRICE
            , A.FREIGHT_VALUE
            , A.SELLER_CITY
            , A.SELLER_STATE
            , A.SELLER_ZIP_CODE_PREFIX
            , A.PRODUCT_CATEGORY_NAME
            , A.PRODUCT_CATEGORY_NAME_ENGLISH
            , A.CUSTOMER_UNIQUE_ID
            , A.CUSTOMER_ZIP_CODE_PREFIX
            , A.PRICE + A.FREIGHT_VALUE      AS PRICE2
            , B.GEOLOCATION_LAT              AS CUST_LAT
            , B.GEOLOCATION_LNG              AS CUST_LNG
    FROM          OTMP2    AS A
    LEFT JOIN     GEO_TBL  AS B
    ON          A.CUSTOMER_ZIP_CODE_PREFIX = B.geolocation_zip_code_prefix
    -- 110197
    )
    , RPT2 AS(
    SELECT A.ORDER_ID
        , A.CUSTOMER_ID
        , A.PUR_YEAR
        , A.REFYM
        , A.order_purchase_timestamp
        , A.order_approved_at
        , A.order_delivered_carrier_date
        , A.order_delivered_customer_date
        , A.order_estimated_delivery_date
        , A.shipping_limit_date
        , A.CARRIER_DIF                -- 승인 후 발송 DLV
        , A.ESTIMATE_DIF  -- 기대 예측
        , A.RELIABL_DIF              -- 신뢰성
        , A.DLV_TIME
        , A.PRODUCT_ID
        , A.SELLER_ID
        , A.PRICE
        , A.FREIGHT_VALUE
        , A.SELLER_CITY
        , A.SELLER_STATE
        , A.SELLER_ZIP_CODE_PREFIX
        , A.PRODUCT_CATEGORY_NAME
        , A.PRODUCT_CATEGORY_NAME_ENGLISH
        , A.CUSTOMER_UNIQUE_ID
        , A.CUSTOMER_ZIP_CODE_PREFIX
        , A.PRICE2
        , A.CUST_LAT
        , A.CUST_LNG
        , B.GEOLOCATION_LAT              AS SELLER_LAT
        , B.GEOLOCATION_LNG              AS SELLER_LNG
    FROM          RPT1    AS A
    LEFT JOIN     GEO_TBL  AS B
    ON          A.SELLER_ZIP_CODE_PREFIX = B.geolocation_zip_code_prefix
    -- 110197
    )
    SELECT * FROM RPT2
    """
    OLST_DLVTBL = spark.sql(qr)
    # OLST_DLVTBL.write.parquet("../bucket/tmp/OLST_DLVTBL.parquet")
    OLST_DLVTBL.createOrReplaceTempView('OLST_DLVTBL') 


    # 2차 가공
    qr = """
        SELECT * FROM OLST_DLVTBL
        """
    dlv_tbl = spark.sql(qr).toPandas()

    ## util 화 필요
    slst = []
    for y, x in zip(dlv_tbl.SELLER_LAT, dlv_tbl.SELLER_LNG):
        start = (float(y), float(x))
        slst.append(start)

    glst = []
    for y, x in zip(dlv_tbl.CUST_LAT, dlv_tbl.CUST_LNG):
        goal = (float(y), float(x))
        glst.append(goal)

    hlst = []
    for start, goal in zip(slst, glst):
        hlst.append(haversine(start, goal))
    ## util 화 필요 (END)

    dlv_tbl['DLV_KM'] = hlst
    dlv_tbl.to_csv('../bucket/tmp/OLST_DLVTBL.csv', index=False, encoding='utf-8-sig')

    tmp = spark.read.csv('../bucket/tmp/OLST_DLVTBL.csv',inferSchema=True,header=True)
    tmp.createOrReplaceTempView("OLST_DLVTBL2")


    qr = """
    SELECT REFYM, SELLER_STATE, SELLER_CITY, SELLER_ID
        , MEAN(CARRIER_DIF) CARRIER_DIF
        , MEAN(ESTIMATE_DIF) ESTIMATE_DIF
        , MEAN(RELIABL_DIF) RELIABL_DIF
        , MEAN(DLV_TIME) DLV_TIME
        , MEAN(DLV_TIME/DLV_KM) DLV_TIME_KM
        , MEAN(DLV_TIME/FREIGHT_VALUE) DLV_TIME_FV
        , COUNT(DISTINCT CUSTOMER_UNIQUE_ID) CNT_CUST
        , COUNT(DISTINCT ORDER_ID) CNT_ORD
        , SUM(PRICE)  SUM_AMT
        , SUM(PRICE2) SUM_AMT2
        , SUM(FREIGHT_VALUE) FREIGHT_VALUE
    FROM OLST_DLVTBL2
    GROUP BY 1,2,3,4
    """
    OLST_DLVSUM = spark.sql(qr)
    OLST_DLVSUM.write.parquet("../bucket/tmp/OLST_DLVTBL_SUM.parquet")

