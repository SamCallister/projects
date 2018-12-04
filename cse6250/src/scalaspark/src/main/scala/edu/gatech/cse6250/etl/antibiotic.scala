package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SparkSession, Dataset }
import org.apache.spark.rdd.RDD

object AntibioticInfo {
  val antibioticRegex = raw".*adoxa.*|.*ala-tet.*|.*alodox.*|.*amikacin.*|.*amikin.*|.*amoxicillin.*|.*amoxicillin.*clavulanate.*|.*clavulanate.*|.*ampicillin.*|.*augmentin.*|.*avelox.*|.*avidoxy.*|.*azactam.*|.*azithromycin.*|.*aztreonam.*|.*axetil.*|.*bactocill.*|.*bactrim.*|.*bethkis.*|.*biaxin.*|.*bicillin l-a.*|.*cayston.*|.*cefazolin.*|.*cedax.*|.*cefoxitin.*|.*ceftazidime.*|.*cefaclor.*|.*cefadroxil.*|.*cefdinir.*|.*cefditoren.*|.*cefepime.*|.*cefotetan.*|.*cefotaxime.*|.*cefpodoxime.*|.*cefprozil.*|.*ceftibuten.*|.*ceftin.*|.*cefuroxime .*|.*cefuroxime.*|.*cephalexin.*|.*chloramphenicol.*|.*cipro.*|.*ciprofloxacin.*|.*claforan.*|.*clarithromycin.*|.*cleocin.*|.*clindamycin.*|.*cubicin.*|.*dicloxacillin.*|.*doryx.*|.*doxycycline.*|.*duricef.*|.*dynacin.*|.*ery-tab.*|.*eryped.*|.*eryc.*|.*erythrocin.*|.*erythromycin.*|.*factive.*|.*flagyl.*|.*fortaz.*|.*furadantin.*|.*garamycin.*|.*gentamicin.*|.*kanamycin.*|.*keflex.*|.*ketek.*|.*levaquin.*|.*levofloxacin.*|.*lincocin.*|.*macrobid.*|.*macrodantin.*|.*maxipime.*|.*mefoxin.*|.*metronidazole.*|.*minocin.*|.*minocycline.*|.*monodox.*|.*monurol.*|.*morgidox.*|.*moxatag.*|.*moxifloxacin.*|.*myrac.*|.*nafcillin sodium.*|.*nicazel doxy 30.*|.*nitrofurantoin.*|.*noroxin.*|.*ocudox.*|.*ofloxacin.*|.*omnicef.*|.*oracea.*|.*oraxyl.*|.*oxacillin.*|.*pc pen vk.*|.*pce dispertab.*|.*panixine.*|.*pediazole.*|.*penicillin.*|.*periostat.*|.*pfizerpen.*|.*piperacillin.*|.*tazobactam.*|.*primsol.*|.*proquin.*|.*raniclor.*|.*rifadin.*|.*rifampin.*|.*rocephin.*|.*smz-tmp.*|.*septra.*|.*septra ds.*|.*septra.*|.*solodyn.*|.*spectracef.*|.*streptomycin sulfate.*|.*sulfadiazine.*|.*sulfamethoxazole.*|.*trimethoprim.*|.*sulfatrim.*|.*sulfisoxazole.*|.*suprax.*|.*synercid.*|.*tazicef.*|.*tetracycline.*|.*timentin.*|.*tobi.*|.*tobramycin.*|.*trimethoprim.*|.*unasyn.*|.*vancocin.*|.*vancomycin.*|.*vantin.*|.*vibativ.*|.*vibra-tabs.*|.*vibramycin.*|.*zinacef.*|.*zithromax.*|.*zmax.*|.*zosyn.*|.*zyvox.*"

  def getFirstAntibioticDate(prescriptions: Dataset[Prescription], cvInputs: Dataset[InputEvent], mvInputs: Dataset[InputEvent], dItems: Dataset[DItem]): DataFrame = {
    val spark = prescriptions.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val antibioticItems = dItems.filter(lower($"label").rlike(antibioticRegex))

    val firstAntibioticMV = mvInputs.filter($"startTime" > 0)
      .join(antibioticItems, mvInputs("itemID") === antibioticItems("itemID"))
      .groupBy("patientID", "hadmID")
      .agg(min($"startTime").alias("dateTime"))

    val firstAntibioticCV = cvInputs.filter($"startTime" > 0)
      .join(antibioticItems, cvInputs("itemID") === antibioticItems("itemID"))
      .groupBy("patientID", "hadmID")
      .agg(min($"startTime").alias("dateTime"))

    val firstAntibioticPrescription = prescriptions.filter($"startDate" > 0)
      .filter(lower($"medicine").rlike(antibioticRegex))
      .groupBy("patientID", "hadmID")
      .agg(min($"startDate").alias("dateTime"))

    val unionedResult = firstAntibioticMV.union(firstAntibioticCV).union(firstAntibioticPrescription)

    val finalResult = unionedResult.groupBy("patientID", "hadmID")
      .agg((min($"dateTime").alias("dateTime")))

    finalResult
  }
}
