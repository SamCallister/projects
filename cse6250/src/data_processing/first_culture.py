import pyspark.sql.functions as func

'''
@args
- ctx: spark context
- df: Microbiology events dataframe
@returns frame (SUBJECT_ID, CULTURE_DATETIME)
'''
def firstCulture(df):
    df = df.select(df['SUBJECT_ID'], func.coalesce(df['CHARTTIME'], df['CHARTDATE']).alias('DATETIME'))

    return df.where(df['DATETIME'].isNotNull()) \
        .groupby('SUBJECT_ID') \
        .agg(func.min(df["DATETIME"]).alias('CULTURE_DATETIME'))