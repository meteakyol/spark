# Databricks notebook source
# MAGIC %md
# MAGIC Get the count of each log level  
# MAGIC Also count the number of empty and unpareseable lines

# COMMAND ----------

log_file = "/FileStore/tables/application.log"

# COMMAND ----------

log_rdd = sc.textFile(log_file)

# COMMAND ----------

def getLogLevel(log_line_iter):
	for log_line in log_line_iter:
		stripped_line = log_line.strip()
		if stripped_line:
			log_tokens = stripped_line.split(" ")
			if(len(log_tokens) > 4):
				log_level = log_tokens[3]
				if log_level.lower() in ['[trace]', '[debug]', '[info]', '[warn]', '[error]', '[fatal]']:
					yield log_level

# COMMAND ----------

log_count_dict = log_rdd.mapPartitions(getLogLevel).countByValue()
for log_level, count in log_count_dict.items():
	print("{} has come {} times".format(log_level, count))

# COMMAND ----------

def getUnparseableLine(log_line_iter):
	for log_line in log_line_iter:
		stripped_line = log_line.strip()
		if stripped_line:
			log_tokens = stripped_line.split(" ")
			if(len(log_tokens) < 4):
				yield 1
			else:
				log_level = log_tokens[3]
				if log_level.lower() not in ['[trace]', '[debug]', '[info]', '[warn]', '[error]', '[fatal]']:
					yield 1

# COMMAND ----------

unparseable_lines_count = log_rdd.mapPartitions(getUnparseableLine).count()

# COMMAND ----------

print("The number of unparseable lines are {}".format(unparseable_lines_count))

# COMMAND ----------

def getEmptyLine(log_line_iter):
	for log_line in log_line_iter:
		stripped_line = log_line.strip()
		if stripped_line == '':
			yield 1

# COMMAND ----------

empty_lines_count = log_rdd.mapPartitions(getEmptyLine).count()

# COMMAND ----------

print("The number of empty lines are {}".format(empty_lines_count))

# COMMAND ----------

# MAGIC %md
# MAGIC #### With accumulators

# COMMAND ----------

log_file = "/FileStore/tables/application.log"

# COMMAND ----------

log_rdd = sc.textFile(log_file)

# COMMAND ----------

unparseable_lines = sc.accumulator(0)
empty_lines = sc.accumulator(0)

# COMMAND ----------

def getLogLevel(log_line_iter):
	global unparseable_lines, empty_lines
	for log_line in log_line_iter:
		stripped_line = log_line.strip()
		if stripped_line:
			log_tokens = stripped_line.split(" ")
			if(len(log_tokens) >= 4):
				log_level = log_tokens[3]
				if log_level.lower() in ['[trace]', '[debug]', '[info]', '[warn]', '[error]', '[fatal]']:
					yield log_level
				else:
					unparseable_lines += 1
			else:
				unparseable_lines += 1
		else:
			empty_lines += 1

# COMMAND ----------

log_count_dict = log_rdd.mapPartitions(getLogLevel).countByValue()
for log_level, count in log_count_dict.items():
	print("{} has come {} times".format(log_level, count))

# COMMAND ----------

print("The number of unparseable lines are {}".format(unparseable_lines.value))

# COMMAND ----------

print("The number of empty lines are {}".format(empty_lines.value))

# COMMAND ----------

sum([count for log_level, count in log_count_dict.items()])

# COMMAND ----------

log_rdd.count()
