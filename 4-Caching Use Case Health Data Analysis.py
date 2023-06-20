# Databricks notebook source
# MAGIC %md
# MAGIC CSV file with Schema:
# MAGIC 
# MAGIC   - id: int
# MAGIC   - gender: str
# MAGIC   - age: float
# MAGIC   - hypertension: int
# MAGIC   - heart_disease: int
# MAGIC   - ever_married: str
# MAGIC   - work_type: str
# MAGIC   - residence_type: str
# MAGIC   - avg_glucose_level: float
# MAGIC   - bmi: float
# MAGIC   - smoking_status: str
# MAGIC   - stroke: int
# MAGIC 
# MAGIC 
# MAGIC __Understand the role of work_type on stroke__  
# MAGIC work_type, count(stroke), count(persons participated for that work type)
# MAGIC 
# MAGIC __Understand the impact of gender on stroke__  
# MAGIC gender, count(stroke), count(persons participated for that gender)
# MAGIC 
# MAGIC __Understand the impact of age group and gender on stroke__  
# MAGIC (age group, gender), count(stroke), count(persons participated for that age group and gender)
# MAGIC 
# MAGIC age category
# MAGIC  - < 5: kid
# MAGIC  - more than or equal to 5 and less than 13: young
# MAGIC  - more than or equal to 13 and less than 20: adolescent
# MAGIC  - more than or equal to 20 and less than 40: adult
# MAGIC  - more than or equal to 40 and less than 50: midage
# MAGIC  - more than or equal to 50 and less than 60: senior
# MAGIC  - more than or equal to 60: old

# COMMAND ----------

healthcare_data_file = "/FileStore/tables/healthcare_dataset_stroke_data.csv"

# COMMAND ----------

healthcare_rdd = sc.textFile(healthcare_data_file)

# COMMAND ----------

# Ignore the first record in the first partition
def skipHeader(index, iter):
  current_row_num = -1
  for record in iter:
    current_row_num += 1
    if index == 0 and current_row_num == 0:
      continue
    yield record

# COMMAND ----------

healthcare_rdd_with_header_removed = healthcare_rdd.mapPartitionsWithIndex(skipHeader)

# COMMAND ----------

healthcare_rdd_with_header_removed.count()

# COMMAND ----------

healthcare_rdd.count()

# COMMAND ----------

healthcare_rdd.take(2)

# COMMAND ----------

healthcare_rdd_with_header_removed.take(2)

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
healthcare_rdd_with_header_removed.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------

# MAGIC %md
# MAGIC **Understand the role of work_type on stroke**

# COMMAND ----------

def getWorkTypeAndStroke(record):
  record_list = record.split(",")
  work_type = str(record_list[6])
  stroke = int(record_list[11])
  return (work_type, stroke)

# COMMAND ----------

def getNumStrokeAndTotalParticipation(val1, val2):
  if isinstance(val1, tuple):
    positive_count, total_count = val1
    if isinstance(val2, tuple):
      positive_count += val2[0]
      total_count += val2[1]
    else:
      positive_count += val2
      total_count += 1
  else:
    positive_count = val1 + val2
    total_count = 2
  return (positive_count, total_count)

# COMMAND ----------

def getPercentagePerKey(pair):
  group, count_info = pair
  if isinstance(count_info, tuple):
    positive_count, total_count = count_info
  else:
    positive_count, total_count = (count_info, 1)
  return (group, positive_count, total_count, float(positive_count * 100) / total_count)

# COMMAND ----------

worktype_stroke = healthcare_rdd_with_header_removed\
  .map(getWorkTypeAndStroke)\
  .reduceByKey(getNumStrokeAndTotalParticipation)\
  .map(getPercentagePerKey)

# COMMAND ----------

for result in worktype_stroke.collect():
  print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC **Understand the role of gender on stroke**

# COMMAND ----------

def getGenderAndStroke(record):
  record_list = record.split(",")
  gender = str(record_list[1])
  stroke = int(record_list[11])
  return (gender, stroke)

# COMMAND ----------

gender_stroke = healthcare_rdd_with_header_removed\
  .map(getGenderAndStroke)\
  .reduceByKey(getNumStrokeAndTotalParticipation)\
  .map(getPercentagePerKey)

# COMMAND ----------

for result in gender_stroke.collect():
  print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC **Understand the impact of age group and gender on stroke**

# COMMAND ----------

def getAgeGroup(age):
  if age < 5:
    return 'kid'
  elif 5 <= age < 13:
    return 'young'
  elif 13 <= age < 20:
    return 'adolescent'
  elif 20 <= age < 40:
    return 'adult'
  elif 40 <= age < 50:
    return 'midage'
  elif 50 <= age < 60:
    return 'senior'
  else:
    return 'old'

# COMMAND ----------

def getAgeGroupGenderAndStroke(record):
  record_list = record.split(",")
  age_group = getAgeGroup(float(record_list[2]))
  gender = str(record_list[1])
  stroke = int(record_list[11])
  return ((age_group, gender), stroke)

# COMMAND ----------

agegroup_gender_stroke = healthcare_rdd_with_header_removed\
  .map(getAgeGroupGenderAndStroke)\
  .reduceByKey(getNumStrokeAndTotalParticipation)\
  .map(getPercentagePerKey)

# COMMAND ----------

for result in agegroup_gender_stroke.collect():
  print(result)

# COMMAND ----------

healthcare_rdd_with_header_removed.unpersist()
