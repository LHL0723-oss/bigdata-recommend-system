from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ===== 创建SparkSession =====
spark = SparkSession.builder \
    .appName("RealTimeAnalysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# ===== 定义数据结构 =====
schema = StructType([
    StructField("student_id", StringType()),
    StructField("textbook_id", StringType()),
    StructField("grade_id", StringType()),
    StructField("subject_id", StringType()),
    StructField("chapter_id", StringType()),
    StructField("question_id", StringType()),
    StructField("score", IntegerType()),
    StructField("answer_time", StringType()),
    StructField("ts", LongType())
])

# ===== 从Kafka读取数据 =====
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "student_answer") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# ===== 实时统计：每个学生的平均分 =====
student_stats = df.groupBy("student_id") \
    .agg(
        avg("score").alias("avg_score"),
        count("question_id").alias("question_count"),
        sum("score").alias("total_score")
    )

# ===== 输出结果到控制台 =====
query = student_stats.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
