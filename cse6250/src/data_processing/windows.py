# def minus48Hours(r):
#     return (r['SUBJECT_ID'], r['DATE'] - timedelta(hours=48))

# startWindow = t1.rdd.map(minus48Hours)

# def add24Hours(r):
#     return (r['SUBJECT_ID'], r['DATE'] + timedelta(hours=24))

# endWindow = t1.rdd.map(add24Hours)