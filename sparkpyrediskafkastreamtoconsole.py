from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# Create a StructType for the Kafka redis-server topic which has all changes made to Redis 
# Before Spark 3.0.0, schema inference is not automatic
redisDBMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(\
            StructType([
                StructField("element", StringType()), \
                StructField("score", StringType())
            ])
        ))
    ]
)

# Create a StructType for the Customer JSON that comes from Redis

customerMessageSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

# Create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis
stdiEventSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("riskDate", StringType())
])

# Create a spark application object
spark = SparkSession.builder.appName("stedi-pipeline-project").getOrCreate()

# Set the spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

# Use the spark application object to read a streaming dataframe from the Kafka topic redis-server as the source

redisDBRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","redis-server")                  \
    .option("startingOffsets","earliest")\
    .load() 

# Cast the key and value columns in the streaming dataframe as a STRING 

redisDBStreamingDF = redisDBRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# Parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
# 
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet

redisDBStreamingDF.withColumn("value",from_json("value",redisDBMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisSortedSet")

# Execute a sql statement against a temporary view, which statement takes the element field from the 0th element 
# in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against 
# a view is different than a dataframe, and it makes it easy to select the 
# nth element of an array in a sql column
zSetEntriesEncodedDF=spark.sql("select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet")

# Take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}

zSetEntriesDecodedDF = zSetEntriesEncodedDF.withColumn("encodedCustomer", unbase64(zSetEntriesEncodedDF.encodedCustomer).cast("string"))


# Parse the JSON in the Customer record and store in a temporary view called CustomerRecords
zSetEntriesDecodedDF.withColumn("encodedCustomer",from_json("encodedCustomer",customerMessageSchema))\
        .select(col('encodedCustomer.*')) \
        .createOrReplaceTempView("CustomerRecords")

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF

emailAndBirthDayStreamingDF = spark.sql("SELECT email, birthDay FROM CustomerRecords WHERE email is not null AND birthDay is not null")

# From the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)

# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select('email',split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

# Sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
# 
# The output should look like this:
# +--------------------+-----               
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct 

emailAndBirthYearStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()