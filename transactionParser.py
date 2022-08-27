from config import *

import systemUtils



class transactionParser():
    def __init__(self, spark_session):
        self.spark = spark_session
        self.sysUtil = systemUtils.systemUtils(self.spark)

    def get_central_account_balance_dict(self, df_accounts_csv):
        return df_accounts_csv \
            .select(OUTPUT_FILE_COLUMNS) \
            .toPandas() \
            .set_index('accountNumber') \
            .to_dict('dict')['balance']

    def get_qualified_transactions(self, df_transactions_csv, df_accounts_csv):
        df_transactions_csv.createOrReplaceTempView("transactions")
        df_accounts_csv.createOrReplaceTempView("accounts")
        df_transactions_accounts = self.spark.sql(TRANSACTIONS_ACCOUNTS_SQL)
        return df_transactions_accounts

    # Call one time in master node
    def compute_new_balance(self, df_transactions, dict_account_balance):
        for row in df_transactions.collect():
            fromAccountBalance = dict_account_balance[row["fromAccountNumber"]]
            toAccountBalance = dict_account_balance[row["toAccountNumber"]]
            transferAmount = row["transferAmount"]
            if (fromAccountBalance >= transferAmount):
                # Deduction for transferTo Account
                dict_account_balance[row["fromAccountNumber"]] = fromAccountBalance - transferAmount
                # Addition for transferTo Account
                dict_account_balance[row["toAccountNumber"]] = toAccountBalance + transferAmount
        return dict_account_balance
