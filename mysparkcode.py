from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark=SparkSession.builder.master('local[*]').getOrCreate()

#Reading another json data which consist complex datatypes.

data1= "C:\BigData\drivers\world_bank.json"
df=spark.read.format('json').option('mode','dropmalformed').load(data1)
#df.printSchema()

# flatten struct data into string/seperate
#res=df.withColumn('theme1name',col('theme1.Name')).withColumn('theme1percent',col('theme1.Percent'))\
  #  .drop('theme1')

#use explode fun flatten the array into struct.
res=df.withColumn('theme1name',col('theme1.Name')).withColumn('theme1percent',col('theme1.Percent'))\
    .drop('theme1').withColumn('theme_namecode',explode(col('theme_namecode'))) \
    .withColumn('theme2code',col('theme_namecode.code')).withColumn('theme2name',col('theme_namecode.name')) \
    .drop('theme_namecode')


