import pyspark.sql.functions as func
from dateutil import parser

def firstDatapoint(patientIds, chartDf):
    patientIds.join(chartDf, patientIds.SUBJECT_ID == chartDf.SUBJECT_ID) \
        .groupby(patientIds['SUBJECT_ID']) \

    c = chartDf.rdd.map(lambda x: x[1]) \
        .distinct() \
        .count()
    print(c)


def getPatientAndDate(r):
    return (r['SUBJECT_ID'], parser.parse(r['CHARTTIME']))

patientAndDate = filtered.rdd.map(getPatientAndDate)

result = patientAndDate.reduceByKey(min)

#result.toDF().coalesce(1).write.format("csv").save(f'{PATH_TO_DATA}first_datapoint.csv')