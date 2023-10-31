from pyspark.sql import SparkSession
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os

spark = SparkSession.builder.master("local[*]").getOrCreate()

## Load Source Data
for dirname, _, filenames in os.walk('../bucket/olist_source'):
    for filename in filenames:
        fn = filename.split('.csv')[0].split('_dataset')[0]
        globals()[fn] = spark.read.csv(os.path.join(dirname, filename),inferSchema=True,header=True)
        globals()[fn].createOrReplaceTempView(f"{fn}")

        print(f"{fn} 's shape is ") #, globals()[fn].shape)

def main(TODAY='20180903', STMTD='201808'):
    """
    TODAY : 집계 마감 일자 YYYYMMDD
    STMTD : 집계 시작 연월 YYYYMM
    """

    ## 1차 가공 SAL_TBL (일자별) + 카테고리 영문명 추가 (23.09.25)

    qr = f"""
    WITH OTMP1 AS( -- 직전 2개월 까지 집계: 배송 완료(구매확정만 담기)
    SELECT A.ORDER_ID
        , A.CUSTOMER_ID
        , A.ORDER_STATUS
        , A.ORDER_PURCHASE_TIMESTAMP
        , SPLIT(A.ORDER_PURCHASE_TIMESTAMP, '-')[0]                            AS PUR_YEAR
        , SPLIT(A.ORDER_PURCHASE_TIMESTAMP, '-')[1]                            AS PUR_MONTH
        , SPLIT(SPLIT(A.ORDER_PURCHASE_TIMESTAMP, '-')[2] , ' ')[0]            AS PUR_DAY
        , SPLIT(SPLIT(A.ORDER_PURCHASE_TIMESTAMP, '-')[2] , ' ')[1]            AS PUR_TIME
        , SUBSTR(DATE_FORMAT(TO_DATE( SPLIT(A.ORDER_PURCHASE_TIMESTAMP,' ')[0], 'yyyy-MM-dd'),'yyyyMMdd'),1,6)  AS REFYM
        , B.ORDER_ITEM_ID
        , B.PRODUCT_ID
        , B.SELLER_ID
        , B.PRICE
        , B.FREIGHT_VALUE
    FROM       OLIST_ORDERS       AS A
    LEFT JOIN  OLIST_ORDER_ITEMS  AS B
    ON         A.ORDER_ID = B.ORDER_ID
    WHERE      A.ORDER_STATUS = 'delivered'
    AND        DATE_FORMAT(TO_DATE( SPLIT(A.ORDER_PURCHASE_TIMESTAMP,' ')[0], 'yyyy-MM-dd'),'yyyyMMdd') < {STMTD}||01
    )
    , OTMP2 AS (
    SELECT A.ORDER_ID
        , A.CUSTOMER_ID
        , A.ORDER_STATUS
        , A.ORDER_PURCHASE_TIMESTAMP
        , A.PUR_YEAR
        , A.PUR_MONTH
        , A.PUR_DAY
        , A.PUR_TIME
        , A.REFYM
        , A.ORDER_ITEM_ID
        , A.PRODUCT_ID
        , A.SELLER_ID
        , A.PRICE
        , A.FREIGHT_VALUE
        , B.SELLER_CITY
        , B.SELLER_STATE
        , C.PRODUCT_CATEGORY_NAME
        , C.PRODUCT_CATEGORY_NAME_ENGLISH
        , D.CUSTOMER_UNIQUE_ID

    FROM        OTMP1           AS A
    LEFT JOIN   OLIST_SELLERS   AS B
    ON          A.SELLER_ID = B.SELLER_ID
    LEFT JOIN   OLST_PRD_TBL    AS C
    ON          A.PRODUCT_ID = C.PRODUCT_ID
    LEFT JOIN   OLIST_CUSTOMERS AS D
    ON          A.CUSTOMER_ID = D.CUSTOMER_ID
    )
    , RPT1 AS (
    SELECT A.ORDER_ID
        , A.CUSTOMER_ID
        , A.ORDER_STATUS
        , A.ORDER_PURCHASE_TIMESTAMP
        , A.PUR_YEAR
        , A.PUR_MONTH
        , A.PUR_DAY
        , A.PUR_TIME
        , A.REFYM
        , A.ORDER_ITEM_ID
        , A.PRODUCT_ID
        , A.SELLER_ID
        , A.PRICE
        , A.FREIGHT_VALUE
        , A.SELLER_CITY
        , A.SELLER_STATE
        , A.PRODUCT_CATEGORY_NAME
        , A.PRODUCT_CATEGORY_NAME_ENGLISH
        , A.CUSTOMER_UNIQUE_ID
        , A.PRICE + A.FREIGHT_VALUE  AS PRICE2
    FROM   OTMP2  AS A
    )
    , RTMP1 AS( -- 직전 2개월 까지 집계
    SELECT A.ORDER_ID
        , A.CUSTOMER_ID
        , A.ORDER_STATUS
        , A.ORDER_PURCHASE_TIMESTAMP
        , SPLIT(A.ORDER_PURCHASE_TIMESTAMP, '-')[0]                            AS PUR_YEAR
        , SPLIT(A.ORDER_PURCHASE_TIMESTAMP, '-')[1]                            AS PUR_MONTH
        , SPLIT(SPLIT(A.ORDER_PURCHASE_TIMESTAMP, '-')[2] , ' ')[0]            AS PUR_DAY
        , SPLIT(SPLIT(A.ORDER_PURCHASE_TIMESTAMP, '-')[2] , ' ')[1]            AS PUR_TIME
        , SUBSTR(DATE_FORMAT(TO_DATE( SPLIT(A.ORDER_PURCHASE_TIMESTAMP,' ')[0], 'yyyy-MM-dd'),'yyyyMMdd'),1,6)  AS REFYM
        , B.ORDER_ITEM_ID
        , B.PRODUCT_ID
        , B.SELLER_ID
        , NVL(B.PRICE ,'0') PRICE
        , NVL(B.FREIGHT_VALUE, '0') FREIGHT_VALUE
    FROM       OLIST_ORDERS       AS A
    LEFT JOIN  OLIST_ORDER_ITEMS  AS B
    ON         A.ORDER_ID = B.ORDER_ID
    WHERE      DATE_FORMAT(TO_DATE( SPLIT(A.ORDER_PURCHASE_TIMESTAMP,' ')[0], 'yyyy-MM-dd'),'yyyyMMdd') BETWEEN {STMTD}||01 AND {TODAY}
    --AND        A.ORDER_STATUS = 'delivered'  -- 최근 매출은 shipped, approved 모두 포함 (구매확정 가능성이 존재하므로)
    )
    , RTMP2 AS (
    SELECT A.ORDER_ID
        , A.CUSTOMER_ID
        , A.ORDER_STATUS
        , A.ORDER_PURCHASE_TIMESTAMP
        , A.PUR_YEAR
        , A.PUR_MONTH
        , A.PUR_DAY
        , A.PUR_TIME
        , A.REFYM
        , A.ORDER_ITEM_ID
        , A.PRODUCT_ID
        , A.SELLER_ID
        , A.PRICE
        , A.FREIGHT_VALUE
        , B.SELLER_CITY
        , B.SELLER_STATE
        , C.PRODUCT_CATEGORY_NAME
        , C.PRODUCT_CATEGORY_NAME_ENGLISH
        , D.CUSTOMER_UNIQUE_ID

    FROM        RTMP1           AS A
    LEFT JOIN   OLIST_SELLERS   AS B
    ON          A.SELLER_ID = B.SELLER_ID
    LEFT JOIN   OLST_PRD_TBL    AS C
    ON          A.PRODUCT_ID = C.PRODUCT_ID
    LEFT JOIN   OLIST_CUSTOMERS AS D
    ON          A.CUSTOMER_ID = D.CUSTOMER_ID
    )
    , RPT2 AS (
    SELECT A.ORDER_ID
        , A.CUSTOMER_ID
        , A.ORDER_STATUS
        , A.ORDER_PURCHASE_TIMESTAMP
        , A.PUR_YEAR
        , A.PUR_MONTH
        , A.PUR_DAY
        , A.PUR_TIME
        , A.REFYM
        , A.ORDER_ITEM_ID
        , A.PRODUCT_ID
        , A.SELLER_ID
        , A.PRICE
        , A.FREIGHT_VALUE
        , A.SELLER_CITY
        , A.SELLER_STATE
        , A.PRODUCT_CATEGORY_NAME
        , A.PRODUCT_CATEGORY_NAME_ENGLISH
        , A.CUSTOMER_UNIQUE_ID
        , A.PRICE + A.FREIGHT_VALUE  AS PRICE2
    FROM   RTMP2  AS A

    )
    (SELECT * FROM RPT1 ORDER BY REFYM)
    UNION ALL
    (SELECT * FROM RPT2 ORDER BY REFYM)
    """
    OLST_PURTBL = spark.sql(qr)
    OLST_PURTBL.write.parquet("../bucket/tmp/OLST_PURTBL.parquet") 
    OLST_PURTBL.createOrReplaceTempView('OLST_PURTBL')

    ## 2차 가공 seller 별 MTD 성과
    qr = """
    WITH MTDTMP AS (
    SELECT   REFYM, SELLER_ID, ORDER_STATUS, SELLER_STATE, SELLER_CITY, PRODUCT_CATEGORY_NAME, PRODUCT_CATEGORY_NAME_ENGLISH
            , COUNT(DISTINCT ORDER_ID)           CNT_ORDER
            , COUNT(DISTINCT CUSTOMER_UNIQUE_ID) CNT_CUST
            , SUM(PRICE) AMT_1
            , SUM(PRICE2) AMT_2
            , SUM(FREIGHT_VALUE) F_VALUE
    FROM     OLST_PURTBL
    GROUP BY  1,2,3,4,5,6,7
    HAVING    SUM(PRICE) > 0
    )
    , RPT1 AS (
    SELECT   REFYM, SELLER_ID, ORDER_STATUS, SELLER_STATE, SELLER_CITY, PRODUCT_CATEGORY_NAME
            , CASE  WHEN PRODUCT_CATEGORY_NAME='portateis_cozinha_e_preparadores_de_alimentos' THEN 'kitchen portals and food preparators'
                    WHEN PRODUCT_CATEGORY_NAME='pc_gamer'                                      THEN 'pc_gamer'      ELSE PRODUCT_CATEGORY_NAME_ENGLISH  END PRODUCT_CATEGORY_NAME_ENGLISH,
            , CASE  WHEN SELLER_STATE='PR' THEN 'Parana'
                    WHEN SELLER_STATE='RJ' THEN 'Rio de Janeiro'
                    WHEN SELLER_STATE='RS' THEN 'Rio Grande do Sul'
                    WHEN SELLER_STATE='SP' THEN 'Sao Paulo'
                    WHEN SELLER_STATE='MG' THEN 'Minas Gerais'
                    WHEN SELLER_STATE='SC' THEN 'Santa Catarina'
                    WHEN SELLER_STATE='ES' THEN 'Espirito Santo'
                    WHEN SELLER_STATE='DF' THEN 'Distrito Federal'
                    WHEN SELLER_STATE='MS' THEN 'Mato Grosso do Sul'
                    WHEN SELLER_STATE='BA' THEN 'Bahia'
                    WHEN SELLER_STATE='CE' THEN 'Ceara'
                    WHEN SELLER_STATE='SE' THEN 'Sergipe'
                    WHEN SELLER_STATE='GO' THEN 'Goias'
                    WHEN SELLER_STATE='PE' THEN 'Pernambuco'
                    WHEN SELLER_STATE='AM' THEN 'Amazonas'
                    WHEN SELLER_STATE='PB' THEN 'Paraiba'
                    WHEN SELLER_STATE='RN' THEN 'Rio Grande do Norte'
                    WHEN SELLER_STATE='RO' THEN 'Rondonia'
                    WHEN SELLER_STATE='MT' THEN 'Mato Grosso'
                    WHEN SELLER_STATE='PA' THEN 'Para'
                    WHEN SELLER_STATE='MA' THEN 'Maranhao'
                    WHEN SELLER_STATE='PI' THEN 'Piaui'
                    WHEN SELLER_STATE='AP' THEN 'Amapa'
                    WHEN SELLER_STATE='AC' THEN 'Acre'
                    WHEN SELLER_STATE='AL' THEN 'Alagoas'
                    WHEN SELLER_STATE='TO' THEN 'Tocantins'
                    WHEN SELLER_STATE='RR' THEN 'Roraima'          ELSE 'CHK' END  AS ID
            , CNT_ORDER
            , CNT_CUST
            , AMT_1
            , AMT_2
            , F_VALUE
            , AMT_1/CNT_CUST UNIT_AMT
    FROM MTDTMP
    )
    SELECT * FROM RPT1
    """
    SAL_SUMM1 = spark.sql(qr)
    SAL_SUMM1.write.parquet("../bucket/tmp/OLST_SAL_SUUMM1.parquet") 

    # SAL_SUMM1.createOrReplaceTempView("SAL_SUMM1")
