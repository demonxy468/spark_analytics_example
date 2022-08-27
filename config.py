# data
ACCOUNT_CSV_PATH = 'data/accounts.csv'
TRANSACTION_CSV_PATH = 'data/transactions.csv'

# spark
## Spark Config
SPARK_APP_NAME = 'roktInterviewApp'
MAXPARTITIONBYTES = 1000000 # 10 MB per-partition

## Spark Job
TRANSACTIONS_ACCOUNTS_SQL = """
                    SELECT /*+ BROADCAST(a,b) */ t.date, t.fromAccountNumber,
                     FLOAT(t.transferAmount), t.toAccountNumber
                     FROM transactions t
                     JOIN accounts a ON a.accountNumber = t.fromAccountNumber
                     JOIN accounts b ON b.accountNumber = t.toAccountNumber
                     WHERE a.country = b.country
                     ORDER BY date asc
                    """

# FileI/O
OUTPUT_FILE_NAME = 'df_account_final'
OUTPUT_FILE_COLUMNS = ['accountNumber', 'balance']
BALANCE_COLUMN = 'balance'
