# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC __nyse_daily.tsv__ (~ 3 MB) has a schema of:
# MAGIC 
# MAGIC   - exchange:string
# MAGIC   - symbol:string
# MAGIC   - date:string
# MAGIC   - open:float
# MAGIC   - high:float
# MAGIC   - low:float
# MAGIC   - close:float
# MAGIC   - volume:int
# MAGIC   - adj_close:float
# MAGIC 
# MAGIC The __nyse_dividends.tsv__ (~ 17 KB) has a schema of
# MAGIC 
# MAGIC   - exchange:string
# MAGIC   - symbol:string
# MAGIC   - date:string
# MAGIC   - dividends:float
# MAGIC 
# MAGIC 
# MAGIC The NYSE data was obtained at http://www.infochimps.com/datasets/nyse-daily-1970-2010-open-close-high-low-and-volume
# MAGIC 
# MAGIC Find out the difference in the stock price on the days dividends were paid out

# COMMAND ----------

nyse_daily_data_file = "/FileStore/tables/nyse_daily.tsv"
nyse_dividends_data_file = "/FileStore/tables/nyse_dividends.tsv"

# COMMAND ----------

def getParsedDailyRecord(daily):
  daily_list = daily.split("\t")
  exchange = str(daily_list[0])
  symbol = str(daily_list[1])
  date = str(daily_list[2])
  open = float(daily_list[3])
  high = float(daily_list[4])
  close = float(daily_list[6])
  return ((exchange, symbol, date), open, close)

# COMMAND ----------

def generateDividendsDictionary(dividend):
  dividend_list = dividend.split("\t")
  exchange = str(dividend_list[0])
  symbol = str(dividend_list[1])
  date = str(dividend_list[2])
  dividends = float(dividend_list[3])
  return ((exchange, symbol, date), dividends)

# COMMAND ----------

daily_rdd = sc.textFile(nyse_daily_data_file)
daily_pair_rdd = daily_rdd.map(getParsedDailyRecord)

# COMMAND ----------

dividends = sc.textFile(nyse_dividends_data_file)

# COMMAND ----------

dividends_pair_rdd = dividends.map(generateDividendsDictionary)
dividends_dict = dict(dividends_pair_rdd.collect())

# COMMAND ----------

dividends_bdct = sc.broadcast(dividends_dict)

# COMMAND ----------

def getDiffAndDividends(daily_pair_iter):
  for daily_pair in daily_pair_iter:
    key, open, close = daily_pair
    dividend = dividends_bdct.value.get(key)
    if dividend is None:
      continue
    else:
      exchange, symbol, date = key
      yield "{} {} {} {} {}".format(exchange, symbol, date, close - open, dividend)

# COMMAND ----------

result_rdd = daily_pair_rdd.mapPartitions(getDiffAndDividends)
for result in result_rdd.take(5):
  print(result)

# COMMAND ----------

result_rdd.count()

# COMMAND ----------

daily_pair_rdd.count()

# COMMAND ----------

dividends_pair_rdd.count()
