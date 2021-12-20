from pyspark.sql import SparkSession
import unittest, sys
from calcBmicountObeseCount import calculateObeseCount


class BMIUnitTest(unittest.TestCase):
    input_path = ''
    output_path = ''

    def test_calculateObeseCount(self):
        spark = SparkSession.builder.appName("Unit Test BmiCalculator").getOrCreate()
        input_path = self.input_path
        output_path = self.output_path
        calculateObeseCount(input_path,output_path)
        expectedresultcount = 3
        actualresultcount = int(spark.read.option("header", "true").csv(output_path+"/finalOutput").rdd.map(list).collect()[0][0])
        self.assertEqual(expectedresultcount, actualresultcount)


if __name__ == '__main__':
    BMIUnitTest.output_path = sys.argv.pop()
    BMIUnitTest.input_path = sys.argv.pop()
    unittest.main()
