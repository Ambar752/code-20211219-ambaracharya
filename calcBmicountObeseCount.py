from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round,countDistinct
import sys, logging, traceback

logging.basicConfig(level=logging.INFO)
class bmiAppFailed(Exception):
    pass
def calculateObeseCount(input_path,output_path):
    try:
        spark=SparkSession.builder.appName("BmiCalculator").getOrCreate()
        sourceDataDF=spark.read.format("json").load(input_path)
        sourceDataDF.repartition(200).withColumn("bmi",round(col("WeightKg")/(col("HeightCm")*0.01),1)).where("HeightCm > 0 AND WeightKg > 0").createOrReplaceTempView("patientsrcdata")
        transformedDataDF=spark.sql("SELECT Gender, HeightCm, WeightKg,bmi `BMI (Body Mass Index)`, " +

          "CASE WHEN bmi <= 18.4 THEN 'Underweight' " +
          "WHEN bmi >= 18.5 AND bmi <= 24.9 THEN 'Normal weight' " +
          "WHEN bmi >= 25 AND bmi <= 29.9 THEN 'Overweight' " +
          "WHEN bmi >= 30 AND bmi <= 34.9 THEN 'Moderately obese' " +
          "WHEN bmi >= 35 AND bmi <= 39.9 THEN 'Severely obese' " +
          "WHEN bmi >= 40 THEN 'Very severely obese' " +
          "END `BMI Category`, " +

          "CASE WHEN bmi <= 18.4 THEN 'Malnutrition risk' " +
          "WHEN bmi >= 18.5 AND bmi <= 24.9 THEN 'Low risk' " +
          "WHEN bmi >= 25 AND bmi <= 29.9 THEN 'Enhanced risk' " +
          "WHEN bmi >= 30 AND bmi <= 34.9 THEN 'Medium risk' " +
          "WHEN bmi >= 35 AND bmi <= 39.9 THEN 'High risk' " +
          "WHEN bmi >= 40 THEN 'Very high risk' " +
          "END `Health risk` " +

          "FROM patientsrcdata WHERE HeightCm != 0 AND WeightKg != 0")

        transformedDataDF.write.option("header", "true").partitionBy("BMI Category").mode("overwrite").format("com.databricks.spark.csv").save(output_path+"/transformedData")
        OverweightDF=spark.read.option("header","true").csv(output_path+"/transformedData").where("`BMI Category` = 'Overweight' ")
        countresult=[{'Total Number of Overweight People': OverweightDF.count()}]
        spark.createDataFrame(countresult).coalesce(1).write.option("header", "true").mode("overwrite").format("com.databricks.spark.csv").save(output_path+"/finalOutput")

        spark.catalog.dropTempView("patientsrcdata")
    except Exception as e:
        traceback.print_exc(300)
        print(e)
        logging.info("***************BMI Calculation Failed*******************")
        raise bmiAppFailed

if __name__ == '__main__':
    input_path=sys.argv[1]
    output_path=sys.argv[2]
    calculateObeseCount(input_path,output_path)

