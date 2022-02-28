from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from os.path import abspath

#warehouse_location = abspath('/user/hive/warehouse')

spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive integration example2321") \
        .config('spark.jars.packages', "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .config("hive.metastore.uris", "thrift://hadoop.spark:9083") \
        .enableHiveSupport() \
        .getOrCreate()

# .config("spark.sql.warehouse.dir", warehouse_location) \

#hc = HiveContext(spark)
#hc.setConf("hive.metastore.uris", "thrift://METASTORE:9083");

#spark = SparkSession.builder.config('spark.jars.packages', "com.microsoft.sqlserver.jdbc.SQLServerDriver").appName("Sql Database connection").getOrCreate()


server_name = "jdbc:sqlserver://172.17.1.11\MSSQLDEV2019"

database_name = "EMS_Kaigarib"
url = server_name + ";" + "databaseName=" + database_name + ";"

username = "kcs"
password = "Krish@123"

'''def fetch_data(sql_query, table):
    fetchDf = hc.read.format("jdbc").option("url", url).option("user", username).option("password", password).option("query", sql_query).option("dbtable", table).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    return fetchDf'''


def fetch_data(sql_query):
    fetchDf = spark.read.format("jdbc").option("url", url).option("user", username).option("password", password).option("query", sql_query).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    return fetchDf

table_list = ["Billing_Run" ]
''', "Cons_Account", "Cons_Name", "POS_CashierReceipt", "POS_CashierReceiptDetails",
              "BIlling_Period", "Cons_Unit", "Const_ServiceType", "Const_Tariff", "Const_AccountType", "Cons_Services",
              "Const_PropertyTypeOfUse", "Const_DocumentType", "Billing_Journal", "Billing_MunicipalityConfiguration",
              "Payroll_Municipality", "Cons_Meter", "Billing_FinancialAccountTransaction", "Billing_JournalTransaction"]'''

#query = "select * from Billing_MunicipalityConfiguration"
#df = fetch_data(query)
#print(df)
#hc.sql("show databases").show()
#hc.sql("show tables").show()

#query = "select * from Cons_Name"
#df = fetch_data(query)
#print(df)
#hc.sql("use lesedilmdebtbook").show()
#df.write.mode("append").saveAsTable("eeee.Cons_Name")
#insertInto
#df.write.mode("append").insertInto("eeee.const_documenttype")
for table in table_list:
    query = "select * from {}".format(table)
    df = fetch_data(query)
    df.write.mode("append").saveAsTable("lesedi.{}".format(table))
    #print(df)
    #test = hc.sql("CREATE TABLE IF NOT EXISTS lesedi_ingest.{}".format(table))
    #hc.sql("show databases").show()
    #hc.sql("create database abc").show()
    #hc.sql("show databases").show()
    #compareDf122 = hc.sql("use abc")
    #test = hc.sql("CREATE TABLE IF NOT EXISTS abc.Billing_MunicipalityConfiguration")
    #compareDf = hc.sql("select * from abc.Billing_MunicipalityConfiguration")
    #df = df.subtract(compareDf)
    
