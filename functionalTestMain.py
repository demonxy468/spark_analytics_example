import systemUtils
import initSparkApp
import transactionParser
import logging
import testCases
import initSparkApp

from pyspark.sql.types import *

try:
    # Initiate Application Contexts
    sparkInit = initSparkApp.initSparkApp()
    spark = sparkInit.initSparkSession()
    tc = testCases.testCaseClass(spark)

    tc.test_happy_path_1()
    tc.test_happy_path_2()
    tc.test_happy_path_3()

    tc.test_insufficient_fund_1()
    tc.test_insufficient_fund_2()

    tc.test_insufficient_sufficient_1()
    tc.test_insufficient_sufficient_2()

    tc.test_cross_country()

except Exception as e:
    logging.error("Fail to process transaction: {}".format(e))
