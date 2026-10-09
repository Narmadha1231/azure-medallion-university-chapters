from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
gold = spark.read.parquet("data/gold/university_chapters/v1")
gold.show(truncate=False)
# Quarantined row must NOT be in Gold
print(gold.filter("chapter_id = 'CA-DQ-Q1'").count())   # expect 0
# Warned row must be in Gold
gold.filter("chapter_id = 'CA-DQ-W1'").select("dq_status", "dq_warnings").show()
spark.read.json("data/quarantine/university_chapters/*").select(
    "chapter_id", "longitude", "quarantine_reason").show(truncate=False)