import pyspark.sql.functions as func

antibioticRegex = '.*adoxa.*|.*ala-tet.*|.*alodox.*|.*amikacin.*|.*amikin.*|.*amoxicillin.*|.*amoxicillin.*clavulanate.*|.*clavulanate.*|.*ampicillin.*|.*augmentin.*|.*avelox.*|.*avidoxy.*|.*azactam.*|.*azithromycin.*|.*aztreonam.*|.*axetil.*|.*bactocill.*|.*bactrim.*|.*bethkis.*|.*biaxin.*|.*bicillin l-a.*|.*cayston.*|.*cefazolin.*|.*cedax.*|.*cefoxitin.*|.*ceftazidime.*|.*cefaclor.*|.*cefadroxil.*|.*cefdinir.*|.*cefditoren.*|.*cefepime.*|.*cefotetan.*|.*cefotaxime.*|.*cefpodoxime.*|.*cefprozil.*|.*ceftibuten.*|.*ceftin.*|.*cefuroxime .*|.*cefuroxime.*|.*cephalexin.*|.*chloramphenicol.*|.*cipro.*|.*ciprofloxacin.*|.*claforan.*|.*clarithromycin.*|.*cleocin.*|.*clindamycin.*|.*cubicin.*|.*dicloxacillin.*|.*doryx.*|.*doxycycline.*|.*duricef.*|.*dynacin.*|.*ery-tab.*|.*eryped.*|.*eryc.*|.*erythrocin.*|.*erythromycin.*|.*factive.*|.*flagyl.*|.*fortaz.*|.*furadantin.*|.*garamycin.*|.*gentamicin.*|.*kanamycin.*|.*keflex.*|.*ketek.*|.*levaquin.*|.*levofloxacin.*|.*lincocin.*|.*macrobid.*|.*macrodantin.*|.*maxipime.*|.*mefoxin.*|.*metronidazole.*|.*minocin.*|.*minocycline.*|.*monodox.*|.*monurol.*|.*morgidox.*|.*moxatag.*|.*moxifloxacin.*|.*myrac.*|.*nafcillin sodium.*|.*nicazel doxy 30.*|.*nitrofurantoin.*|.*noroxin.*|.*ocudox.*|.*ofloxacin.*|.*omnicef.*|.*oracea.*|.*oraxyl.*|.*oxacillin.*|.*pc pen vk.*|.*pce dispertab.*|.*panixine.*|.*pediazole.*|.*penicillin.*|.*periostat.*|.*pfizerpen.*|.*piperacillin.*|.*tazobactam.*|.*primsol.*|.*proquin.*|.*raniclor.*|.*rifadin.*|.*rifampin.*|.*rocephin.*|.*smz-tmp.*|.*septra.*|.*septra ds.*|.*septra.*|.*solodyn.*|.*spectracef.*|.*streptomycin sulfate.*|.*sulfadiazine.*|.*sulfamethoxazole.*|.*trimethoprim.*|.*sulfatrim.*|.*sulfisoxazole.*|.*suprax.*|.*synercid.*|.*tazicef.*|.*tetracycline.*|.*timentin.*|.*tobi.*|.*tobramycin.*|.*trimethoprim.*|.*unasyn.*|.*vancocin.*|.*vancomycin.*|.*vantin.*|.*vibativ.*|.*vibra-tabs.*|.*vibramycin.*|.*zinacef.*|.*zithromax.*|.*zmax.*|.*zosyn.*|.*zyvox.*'

'''
@returns frame (SUBJECT_ID, ANTIBIOTIC_DATETIME)
'''
def firstAntibiotic(itemsDf, inputMVFrame, inputCVFrame):
    antibioticItems = itemsDf.select(itemsDf['ITEMID'], itemsDf['LABEL']) \
        .where(func.lower(itemsDf['LABEL']).rlike(antibioticRegex))

    firstAntibioticMV = inputMVFrame.join(antibioticItems, inputMVFrame.ITEMID == antibioticItems.ITEMID) \
        .groupby('SUBJECT_ID') \
        .agg(func.min(inputMVFrame['STARTTIME']).alias('ANTIBIOTIC_DATETIME'))

    firstAntibioticCV = inputCVFrame.join(antibioticItems, inputCVFrame.ITEMID == antibioticItems.ITEMID) \
        .groupby('SUBJECT_ID') \
        .agg(func.min(inputCVFrame['CHARTTIME']).alias('ANTIBIOTIC_DATETIME'))

    unionedResult = firstAntibioticMV.union(firstAntibioticCV)

    finalResult = unionedResult.groupby('SUBJECT_ID') \
        .agg(func.min(unionedResult['ANTIBIOTIC_DATETIME']).alias('ANTIBIOTIC_DATETIME'))

    
    return finalResult