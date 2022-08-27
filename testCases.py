import systemUtils
import transactionParser
import inspect

from pyspark.sql.types import *


class testCaseClass():
    def __init__(self, spark_session):
        self.spark = spark_session
        # Initiate Application Contexts
        self.utils = systemUtils.systemUtils(self.spark)
        self.parser = transactionParser.transactionParser(self.spark)
        # Read Raw data into the system
        self.df_accounts_csv = self.utils.convert_df_column_tpye(self.utils.read_csv_to_spark_df('data/accounts.csv'),
                                                                 "balance", FloatType())

    # Happy Path Test
    def test_happy_path_1(self):
        try:
            testdata_happy_path_df_1 = self.spark.createDataFrame(
                [
                    ("2021-11-29T05:47:26", "1372", 42, "1300", 1, 1),
                    ("2021-11-29T06:47:26", "1372", 41, "1300", 2, 1),
                    ("2021-11-29T07:47:26", "1372", 40, "1032", 60, 1)
                ],
                ["date", "fromAccountNumber", "fromBalance", "toAccountNumber", "toBalance", "transferAmount", ]
            )

            df_qualified_transactions = self.parser.get_qualified_transactions(testdata_happy_path_df_1,
                                                                               self.df_accounts_csv)
            # df_qualified_transactions.show()
            # Run computation based on business logic
            dict_central_account_balance = self.parser.get_central_account_balance_dict(self.df_accounts_csv)
            res = self.parser.compute_new_balance(df_qualified_transactions, dict_central_account_balance)

            # Balance(Account #1372)  = 42 - 1 - 1 -1 = 39 (Accepted)
            assert res["1372"] == 39.0

            # Balance(Account #1300)  = 1 + 1 + 1 = 3 (Accepted)
            assert res["1300"] == 3.0

            # Balance(Account #1032)  = 60 + 1 = 61 (Accepted)
            assert res["1032"] == 61.0

        except Exception as e:
            raise AssertionError("Test {} failed".format(inspect.stack()[0][3]))

    def test_happy_path_2(self):
        try:
            testdata_happy_path_df_2 = self.spark.createDataFrame(
                [
                    ("2021-11-29T05:47:26", "1372", 42, "1300", 1, 40),
                    ("2021-11-29T06:47:26", "1372", 2, "1300", 41, 2),
                    ("2021-11-29T07:47:26", "1372", 0, "1032", 60, 5)
                ],
                ["date", "fromAccountNumber", "fromBalance", "toAccountNumber", "toBalance", "transferAmount", ]
            )

            df_qualified_transactions = self.parser.get_qualified_transactions(testdata_happy_path_df_2,
                                                                               self.df_accounts_csv)
            # Run computation based on business logic
            dict_central_account_balance = self.parser.get_central_account_balance_dict(self.df_accounts_csv)
            res = self.parser.compute_new_balance(df_qualified_transactions, dict_central_account_balance)

            # Balance(Account #1372)  = 42 - 40 = 2 (Accepted)
            # Balance(Account #1372)  = 2 - 2 = 0 (Accepted)
            # Balance(Account #1372)  = 2 - 5 => 0 (Rejected due to insufficient fund from Acct#1372)
            assert res["1372"] == 0.0

            # Balance(Account #1300)  = 1 + 40 = 41 (Accepted)
            # Balance(Account #1300)  = 41 + 2 = 43 (Accepted)
            assert res["1300"] == 43.0

            # Balance(Account #1032)  = 60 + 5 => 60 (Rejected due to insufficient fund from Acct#1372)
            assert res["1032"] == 60.0

        except Exception as e:
            raise AssertionError("Test {} failed".format(inspect.stack()[0][3]))

    def test_happy_path_3(self):
        try:
            testdata_happy_path_df_3 = self.spark.createDataFrame(
                [
                    ("2021-11-29T05:47:26", "1372", 42, "1300", 1, 40),
                    ("2021-11-29T06:47:26", "1300", 41, "1032", 60, 40),
                    ("2021-11-29T07:47:26", "1032", 100, "1884", 5, 5)
                ],
                ["date", "fromAccountNumber", "fromBalance", "toAccountNumber", "toBalance", "transferAmount", ]
            )

            df_qualified_transactions = self.parser.get_qualified_transactions(testdata_happy_path_df_3,
                                                                               self.df_accounts_csv)
            # Run computation based on business logic
            dict_central_account_balance = self.parser.get_central_account_balance_dict(self.df_accounts_csv)
            res = self.parser.compute_new_balance(df_qualified_transactions, dict_central_account_balance)

            # Balance(Account #1372)  = 42 - 40 = 2 (Accepted)
            assert res["1372"] == 2.0

            # Balance(Account #1300)  = 1 + 40 = 41 (Accepted)
            # Balance(Account #1300)  = 41 - 40 = 1 (Accepted)
            assert res["1300"] == 1.0

            # Balance(Account #1032)  = 60 + 40 = 100 (Accepted)
            # Balance(Account #1032)  = 100 - 5 = 95 (Accepted)
            assert res["1032"] == 95.0

            # Balance(Account #1884)  = 45 + 1 - 1 = 45 (Accepted)
            assert res["1884"] == 10.0

        except Exception as e:
            raise AssertionError("Test {} failed".format(inspect.stack()[0][3]))

    # Insufficient Fund Test
    def test_insufficient_fund_1(self):
        try:
            testdata_insufficient_fund_df_1 = self.spark.createDataFrame(
                [
                    ("2021-11-29T05:47:26", "1372", 42, "1300", 1, 42),
                    ("2021-11-29T06:47:26", "1372", 0, "1300", 41, 2),
                    ("2021-11-29T07:47:26", "1372", 0, "1032", 60, 5)
                ],
                ["date", "fromAccountNumber", "fromBalance", "toAccountNumber", "toBalance", "transferAmount", ]
            )

            df_qualified_transactions = self.parser.get_qualified_transactions(testdata_insufficient_fund_df_1,
                                                                               self.df_accounts_csv)
            # Run computation based on business logic
            dict_central_account_balance = self.parser.get_central_account_balance_dict(self.df_accounts_csv)
            res = self.parser.compute_new_balance(df_qualified_transactions, dict_central_account_balance)

            # Balance(Account #1372)  = 42 - 42 = 0 (Accepted)
            assert res["1372"] == 0.0

            # Balance(Account #1300)  = 1 + 42 = 43 (Accepted)
            # Balance(Account #1300)  = 43 + 2 => 43 (Rejected due to insufficient fund from Acct#1372)
            assert res["1300"] == 43.0

            # Balance(Account #3)  = 60 + 5 => 60 (Rejected due to insufficient fund from Acct#1372)
            assert res["1032"] == 60.0

        except Exception as e:
            raise AssertionError("Test {} failed".format(inspect.stack()[0][3]))

    def test_insufficient_fund_2(self):
        try:
            testdata_insufficient_fund_df_2 = self.spark.createDataFrame(
                [
                    ("2021-11-29T05:47:26", "1300", 1, "1372", 42, 5),
                    ("2021-11-29T06:48:26", "1300", 1, "1372", 42, 1)
                ],
                ["date", "fromAccountNumber", "fromBalance", "toAccountNumber", "toBalance", "transferAmount", ]
            )

            df_qualified_transactions = self.parser.get_qualified_transactions(testdata_insufficient_fund_df_2,
                                                                               self.df_accounts_csv)
            # Run computation based on business logic
            dict_central_account_balance = self.parser.get_central_account_balance_dict(self.df_accounts_csv)
            res = self.parser.compute_new_balance(df_qualified_transactions, dict_central_account_balance)

            # Balance(Account #1300)  = 1 - 5 => 1 (Rejected due to insufficient fund from Acct#1300)
            # Balance(Account #1300)  = 1 - 1 = 0 (Accepted at the second transaction)
            assert res["1300"] == 0.0

            # Balance(Account #2)  = 42 + 1 = 43 (Accepted)
            assert res["1372"] == 43.0

        except Exception as e:
            raise AssertionError("Test {} failed".format(inspect.stack()[0][3]))

    # Insufficient Fund first but sufficient later
    def test_insufficient_sufficient_1(self):
        try:
            testdata_insufficient_sufficient_1_df_1 = self.spark.createDataFrame(
                [
                    ("2021-11-29T05:47:26", "1300", 1, "1372", 42, 5),
                    ("2021-11-29T06:47:26", "1372", 42, "1300", 1, 5),
                    ("2021-11-29T07:48:26", "1300", 6, "1372", 37, 5)
                ],
                ["date", "fromAccountNumber", "fromBalance", "toAccountNumber", "toBalance", "transferAmount", ]
            )

            df_qualified_transactions = self.parser.get_qualified_transactions(testdata_insufficient_sufficient_1_df_1,
                                                                               self.df_accounts_csv)
            # Run computation based on business logic
            dict_central_account_balance = self.parser.get_central_account_balance_dict(self.df_accounts_csv)
            res = self.parser.compute_new_balance(df_qualified_transactions, dict_central_account_balance)

            # Balance(Account #1300)  = 1 - 5 => 1 (Rejected due to insufficient fund from Acct#1300)
            # Balance(Account #1300)  = 1 + 5 = 6 (Accepted transfer from account #1372)
            # Balance(Account #1300)  = 6 - 5 = 1 (Accepted transfer to account #1372)
            assert res["1300"] == 1

            # Balance(Account #1372)  = 42 + 5 => 42 (Rejected due to insufficient fund from Acct#1300)
            # Balance(Account #1372)  = 42 - 5 = 37 (Accepted transfer to account #1300)
            # Balance(Account #1372)  = 37 + 5 = 42 (Accepted transfer from account #1300)
            assert res["1372"] == 42

        except Exception as e:
            raise AssertionError("Test {} failed".format(inspect.stack()[0][3]))

    def test_insufficient_sufficient_2(self):
        try:
            testdata_insufficient_sufficient_df_2 = self.spark.createDataFrame(
                [
                    ("2021-11-29T05:47:26", "1300", 1, "1372", 42, 5),  # (Rejected)
                    ("2021-11-29T06:47:26", "1372", 42, "1300", 1, 5),
                    ("2021-11-29T07:48:26", "1300", 6, "1372", 37, 5),
                    ("2021-11-29T08:48:26", "1300", 1, "1032", 88, 5),  # (Rejected)
                    ("2021-11-29T09:48:26", "1372", 42, "1300", 1, 12),
                    ("2021-11-29T09:50:26", "1300", 13, "1032", 88, 10)
                ],
                ["date", "fromAccountNumber", "fromBalance", "toAccountNumber", "toBalance", "transferAmount", ]
            )

            df_qualified_transactions = self.parser.get_qualified_transactions(testdata_insufficient_sufficient_df_2,
                                                                               self.df_accounts_csv)
            # Run computation based on business logic
            dict_central_account_balance = self.parser.get_central_account_balance_dict(self.df_accounts_csv)
            res = self.parser.compute_new_balance(df_qualified_transactions, dict_central_account_balance)

            # Balance(Account #1300) = 1 - 5 => 1 (Rejected due to insufficient fund from Acct#1300)
            # Balance(Account #1300)  = 1 + 5 = 6 (Accepted transfer from account #1372)
            # Balance(Account #1300)  = 6 - 5 = 1 (Accepted transfer to account #1372)
            # Balance(Account #1300)  = 1 + 12 = 13 (Accepted transfer from account #1372)
            # Balance(Account #1300)  = 13 - 10 = 3 (Accepted transfer to account #1302)
            assert res["1300"] == 3.0

            # Balance(Account #1372)  = 42 + 5 => 42 (Rejected due to insufficient fund from Acct#1300)
            # Balance(Account #1372)  = 42 - 5 = 37 (Accepted transfer to account #1300)
            # Balance(Account #1372)  = 37 + 5 = 42 (Accepted transfer to account #1300)
            # Balance(Account #1372)  = 42 - 12 = 30 (Accepted transfer to account #1300)
            assert res["1372"] == 30

            # # Balance(Account #1302)  = 60 + 5 => 60 (Rejected due to insufficient fund from Acct#1300)
            # # Balance(Account #1302)  = 60 + 10 = 70 (Accepted transfer from account #2)
            assert res["1032"] == 70.0

        except Exception as e:
            raise AssertionError("Test {} failed".format(inspect.stack()[0][3]))

    # Cross Country Test
    def test_cross_country(self):
        try:
            testdata_cross_country_df = self.spark.createDataFrame(
                [
                    ("2021-11-29T05:47:26", "0", 70, "1300", 4, 1),
                    ("2021-11-29T06:47:26", "1", 4, "1300", 70, 1)
                ],
                ["date", "fromAccountNumber", "fromBalance", "toAccountNumber", "toBalance", "transferAmount", ]
            )

            df_qualified_transactions = self.parser.get_qualified_transactions(testdata_cross_country_df,
                                                                               self.df_accounts_csv)

            df_accounts_csv = self.utils.convert_df_column_tpye(self.utils.read_csv_to_spark_df('data/accounts.csv'),
                                                                "balance", FloatType())
            dict_central_account_balance = self.parser.get_central_account_balance_dict(df_accounts_csv)

            res = self.parser.compute_new_balance(df_qualified_transactions, dict_central_account_balance)

            # Balance(Account #0)  = 70 - 1 => 70 (Reject due to cross country)
            # Balance(Account #0)  = 70 + 1 => 70 (Reject due to cross country)
            assert res["0"] == 70

            # Balance(Account #0)  = 4 + 1 => 4 (Reject due to cross country)
            # Balance(Account #0)  = 4 - 1 => 4 (Reject due to cross country)
            assert res["1"] == 4

        except Exception as e:
            raise AssertionError("Test {} failed".format(inspect.stack()[0][3]))
