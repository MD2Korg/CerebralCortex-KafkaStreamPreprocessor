from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

ss = SparkSession.builder
ss.appName("Cassandra CC independent test")
sparkSession = ss.getOrCreate()
sc = sparkSession.sparkContext
sqlContext = SQLContext(sc)

# sample data to test
# dp = [(str("59feb8a0-1137-4dde-99ea-6af384aa9866"), 20170907, 1504810100000, 1504810199000, "123")]
# temp_RDD = sc.parallelize(dp)
# dataframe_data = sqlContext.createDataFrame(temp_RDD,schema=["identifier", "day", "start_time", "end_time","sample"]).coalesce(400)


def store_to_cassandra(dataframe_data):
    dataframe_data.write.format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="data", keyspace="cerebralcortex").save()
