from pyspark.sql.types import *
from config import *

import systemUtils
import initSparkApp
import transactionParser
import logging

try:
    # Initiate Application Contexts
    sparkInit = initSparkApp.initSparkApp()
    spark = sparkInit.initSparkSession()
    utils = systemUtils.systemUtils(spark)
    parser = transactionParser.transactionParser(spark)

    # Read Raw data into the system
    df_transactions_csv = utils.read_csv_to_spark_df(TRANSACTION_CSV_PATH)
    df_accounts_csv = utils.read_csv_to_spark_df(ACCOUNT_CSV_PATH)
    df_accounts_csv = utils.convert_df_column_tpye(df_accounts_csv, BALANCE_COLUMN, FloatType())

    # Pre-process raw data
    dict_central_account_balance = parser.get_central_account_balance_dict(df_accounts_csv)
    df_qualified_transactions = parser.get_qualified_transactions(df_transactions_csv, df_accounts_csv)

    # Run computation based on business logic
    dict_account_final = parser.compute_new_balance(df_qualified_transactions, dict_central_account_balance)
    # Output computation result to CSV file
    # utils.write_dict_to_csv(dict_account_final, OUTPUT_FILE_COLUMNS, OUTPUT_FILE_NAME)
    df_account_final = utils.convert_dict_to_sparkDF(dict_account_final, ["accountNumber", "balance"])
    utils.write_df_to_csv(df_account_final, "", OUTPUT_FILE_NAME, 1)

    sparkInit.stopSparkSession()

except Exception as e:
    logging.error("Fail to process transaction: {}".format(e))
    sparkInit.stopSparkSession()
