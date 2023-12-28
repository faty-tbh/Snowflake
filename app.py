import pandas as pd
import snowflake.connector as sf

from myconfig import myaccount, mypassword, myuser

# Connect to Snowflake
cnn = sf.connect(
    user=myuser,
    password=mypassword,
    account=myaccount
)

cs = cnn.cursor()

# Get Snowflake version
try:
    cs.execute('SELECT current_version()')
    row = cs.fetchone()
    print(f'Current Snowflake Version: {row[0]}')
    print('Creating warehouse...')
    sql= 'CREATE WAREHOUSE IF NOT EXISTS rcw_warehouse'
    cs.execute(sql)
    sql='USE WAREHOUSE rcw_warehouse'
    cs.execute(sql)
    print("CREATE DATABASE...")
    sql='CREATE DATABASE IF NOT EXISTS rcw_database'
    cs.execute(sql)
    sql='USE DATABASE rcw_database'
    cs.execute(sql)
    print("CREATE SCHEMA...")
    sql='CREATE SCHEMA IF NOT EXISTS rcw_schema'
    cs.execute(sql)
    sql='USE SCHEMA rcw_schema'
    cs.execute(sql)
    print("CREATE TABLE...")
    sql=("CREATE OR REPLACE TABLE comments"
        "(Id integer, table_comments string)")
    cs.execute(sql)
    print('Inserting some rows ...')
    sql=("INSERT INTO comments (Id, table_comments)"
         "VALUES (1, 'comment one')")
    cs.execute(sql)
    sql=("INSERT INTO comments (Id, table_comments)"
         "VALUES (2, 'comment two')")
    cs.execute(sql)
    sql=("INSERT INTO comments (Id, table_comments)"
         "VALUES (3, 'comment three')")
    cs.execute(sql)
    print('Reading data ..')
    sql='SELECT * FROM comments'
    cs.execute(sql)
    data= cs.fetchall()
    print('Display all..')
    print(data)
    print('Display one by one')
    for i, row in enumerate(data):
        print(f'Instance {i} - {row}')
finally:
    cs.close()