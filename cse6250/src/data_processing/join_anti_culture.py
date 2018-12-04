joined = firstBiotic.join(firstCulture, firstBiotic.SUBJECT_ID_B == firstCulture.SUBJECT_ID_C, 'fullouter')
joined = joined.select(func.coalesce(joined['SUBJECT_ID_B'], joined['SUBJECT_ID_C']).alias('SUBJECT_ID'), joined['B_DATE'], joined['C_DATE'])

def getMinDate(r):
    key, dateOne, dateTwo = r
    if dateOne is None:
        return (key, dateTwo)
    if dateTwo is None:
        return (key, dateOne)
    
    return (key,min(dateOne, dateTwo))

final = joined.rdd.map(getMinDate)