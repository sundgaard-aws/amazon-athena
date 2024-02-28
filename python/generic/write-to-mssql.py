import datetime
import boto3
import pandas as pd
from pyspark.sql import SparkSession
import os
import pyodbc
import sqlalchemy
from sqlalchemy.engine import URL

print("*** Write CSV to MSSQL ***")
print("Started at ["+str(datetime.datetime.now())+"]...")

db_hostname="mssql.cbjhhlvll9rd.eu-central-1.rds.amazonaws.com"
port=1433
database="mydb"
username="admin"
password="BCxY2y?*%FzYoJVz~u>kZP-2[W0a"
data_table="dummy"
jdbc_url = "jdbc:sqlserver://"+db_hostname+";databaseName="+database+""
connection_details = { "user": username, "password": password, "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",  "trustServerCertificate": "true"}

def spark_init():
    #javaHome="C:\\Program Files\\OpenJDK\\jdk-20"
    javaHome="C:\\Program Files\\Amazon Corretto\\jdk11.0.18_10"
    os.environ["JAVA_HOME"]=javaHome
    hadoopHome="C:\\Users\\sundgaar\\apps\\hadoop-common-2.2.0-bin-master"
    os.environ["HADOOP_HOME"]=hadoopHome
    #os.environ["LD_LIBRARY_PATH"]=str(hadoopHome+"\\bin")
    os.environ["hadoop.home.dir"]=hadoopHome

    spark = SparkSession \
    .builder \
    .appName('SparkConnect') \
    .enableHiveSupport() \
    .config("spark.driver.extraClassPath", "../../drivers/mssql-jdbc-12.2.0.jre11.jar") \
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def read_data_from_s3():
    bucket = "trade-data-s3"
    file_name = "trades/fx-trades-large.csv"
    s3 = boto3.client("s3") 
    obj = s3.get_object(Bucket=bucket, Key=file_name) 
    df = pd.read_csv(obj["Body"], index_col=False)
    print(df)
    print(type(df))    
    return df

def read_data_from_mssql_pyspark():    
    print("reading from mssql via pyspark...")
    mssqlDF=spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    print(mssqlDF)
    print("data read from mssql.")

def write_data_to_mssql_pyspark(df):
    print("writing to mssql via pyspark...")
    dataTable="trade"
    targetSparkDF=spark.createDataFrame(df) 
    targetSparkDF.printSchema()
    targetSparkDF.show()
    targetSparkDF.write.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    print(type(targetSparkDF))
    print("data written to mssql.")

def spark_cleanup(spark):
    spark.stop()
    spark.sparkContext.stop()
    exit(0)

def write_data_to_mssql_pyodbc(df):
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+db_hostname+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    for index, row in df.iterrows():
        cursor.execute("INSERT INTO trade (trade_type,amount,ccy) values(?,?,?)", row.trade_type, row.amount, row.ccy)
    cnxn.commit()
    cursor.close()

def write_data_to_mssql_sqlalchemy(df:pd.DataFrame):
    #df["guid"]=None
    #df=df.head(20)
    #df=df.set_index("guid")
    #print(df)
    connection_url = URL.create(
    "mssql+pyodbc",
    username=username,
    password=password,
    host=db_hostname,
    port=port,
    database=database,
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "TrustServerCertificate": "yes"
    },
    )
    engine = sqlalchemy.create_engine(connection_url)    
    df.to_sql("trade", engine)

df=read_data_from_s3()
write_data_to_mssql_sqlalchemy(df)
#write_data_to_mssql_pyodbc(df)
#spark=spark_init()
#spark_cleanup(spark)

print("*** END OF Write CSV to MSSQL ***")