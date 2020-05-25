from pyspark.sql import SparkSession
from pyspark import SparkFiles
from IPython.display import display
import pyspark.sql.functions as f

spark = SparkSession.builder.appName('summer-project').getOrCreate()

last_15_minutes_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
spark.sparkContext.addFile(last_15_minutes_url)

last_15_minutes_df = (spark.read
    .text("file://"+SparkFiles.get("lastupdate.txt"))
    .withColumn("id", f.split("value", " ")[0])
    .withColumn("code", f.split("value", " ")[1])
    .withColumn("url", f.split("value", " ")[2])
)

display(last_15_minutes_df.select("id", "code", "url").show())
