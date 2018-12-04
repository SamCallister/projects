from itertools import islice

'''
Reads a CVS file as a dataframe.

@args
- ctx: spark context
- absPath: absolute path to file on local machine eg /some/data
- tableName: Name given to the dataframe

@returns spark dataframe
'''
def readDataAsTable(ctx, absPath, tableName, header):
    table = ctx.read.format('com.databricks.spark.csv') \
        .options(header=header, inferschema='true') \
        .load('file://{0}'.format(absPath))
    
    table.createOrReplaceTempView(tableName)

    return table

def readDataIntoRDD(ctx, absPath):
    res = ctx.read.format('csv') \
        .options(header='true') \
        .load('file://{0}'.format(absPath))
    
    return res