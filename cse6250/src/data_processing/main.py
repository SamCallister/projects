from pyspark.sql import SparkSession
from pyspark import SparkContext
from loader import readDataAsTable, readDataIntoRDD
from first_culture import firstCulture
from first_antibiotic import firstAntibiotic
from first_datapoint import firstDatapoint
import pyspark.sql.functions as func

PATH_TO_DATA = '/project/data/'

ss = SparkSession.builder \
    .master('local[*]') \
    .appName('Big Data Project') \
    .config('spark.executor.memory', '8G') \
    .config('spark.driver.memory', '2G') \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.kryoserializer.buffer', '24') \
    .getOrCreate()

#labFrame = readDataAsTable(ss, f'{PATH_TO_DATA}LABEVENTS.csv', 'labs')
bioFrame = readDataAsTable(ss, f'{PATH_TO_DATA}MICROBIOLOGYEVENTS.csv', 'bio')
inputMVFrame = readDataAsTable(ss, f'{PATH_TO_DATA}INPUTEVENTS_MV.csv', 'inputMV')
inputCVFrame = readDataAsTable(ss, f'{PATH_TO_DATA}INPUTEVENTS_CV.csv', 'inputCV')
itemsDict = readDataAsTable(ss, f'{PATH_TO_DATA}D_ITEMS.csv', 'items')


firstCultureDate = firstCulture(bioFrame)
firstAntibioticDate = firstAntibiotic(itemsDict, inputMVFrame, inputCVFrame)

res = firstCultureDate.join(firstAntibiotic, firstCultureDate.SUBJECT_ID == firstAntibioticDate.SUBJECT_ID)

res = res.groupby(res['SUBJECT_ID']) \
    .agg(func.min(res['STARTTIME'], res['ANTIBIOTIC_DATETIME']).alias('CULTURE_DATETIME'))

print(res.count())
print(res.take(5))

patientRDD = readDataIntoRDD(ss, '{0}PATIENTS.csv'.format(PATH_TO_DATA))
chartRDD = readDataIntoRDD(ss, '{0}CHARTEVENTS.csv'.format(PATH_TO_DATA))

# firstDatapointDate = firstDatapoint(patientRDD, chartRDD)



