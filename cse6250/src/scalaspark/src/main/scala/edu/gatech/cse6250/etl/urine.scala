/*

We got this SQL file from
  https://raw.githubusercontent.com/MIT-LCP/mimic-code/master/concepts/firstday/urine-output-first-day.sql


-- ------------------------------------------------------------------
-- Purpose: Create a view of the urine output for each ICUSTAY_ID over the first 24 hours.
-- ------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS uofirstday CASCADE;
create materialized view uofirstday as
select
  -- patient identifiers
  ie.subject_id, ie.hadm_id, ie.icustay_id

  -- volumes associated with urine output ITEMIDs
  , sum(
      -- we consider input of GU irrigant as a negative volume
      case
        when oe.itemid = 227488 and oe.value > 0 then -1*oe.value
        else oe.value
    end) as UrineOutput
from icustays ie
-- Join to the outputevents table to get urine output
left join outputevents oe
-- join on all patient identifiers
on ie.subject_id = oe.subject_id and ie.hadm_id = oe.hadm_id and ie.icustay_id = oe.icustay_id
-- and ensure the data occurs during the first day
and oe.charttime between ie.intime and (ie.intime + interval '1' day) -- first ICU day
where itemid in
(
-- these are the most frequently occurring urine output observations in CareVue
40055, -- "Urine Out Foley"
43175, -- "Urine ."
40069, -- "Urine Out Void"
40094, -- "Urine Out Condom Cath"
40715, -- "Urine Out Suprapubic"
40473, -- "Urine Out IleoConduit"
40085, -- "Urine Out Incontinent"
40057, -- "Urine Out Rt Nephrostomy"
40056, -- "Urine Out Lt Nephrostomy"
40405, -- "Urine Out Other"
40428, -- "Urine Out Straight Cath"
40086,--	Urine Out Incontinent
40096, -- "Urine Out Ureteral Stent #1"
40651, -- "Urine Out Ureteral Stent #2"

-- these are the most frequently occurring urine output observations in MetaVision
226559, -- "Foley"
226560, -- "Void"
226561, -- "Condom Cath"
226584, -- "Ileoconduit"
226563, -- "Suprapubic"
226564, -- "R Nephrostomy"
226565, -- "L Nephrostomy"
226567, --	Straight Cath
226557, -- R Ureteral Stent
226558, -- L Ureteral Stent
227488, -- GU Irrigant Volume In
227489  -- GU Irrigant/Urine Volume Out
)
group by ie.subject_id, ie.hadm_id, ie.icustay_id
order by ie.subject_id, ie.hadm_id, ie.icustay_id;

*/

package edu.gatech.cse6250.etl
import edu.gatech.cse6250.model._
import org.apache.spark.rdd.RDD

object UrineInfo {

  //  Urine codes among itemId's.
  //  Shall we remove GU Irrigant codes 227488, 227489 ?
  val urineCodes = Set(40055L, 43175L, 40069L, 40094L, 40715L, 40473L, 40085L, 40057L, 40056L, 40405L, 40428L, 40086L, 40096L, 40651L, 226559L, 226560L, 226561L, 226584L, 226563L, 226564L, 226565L, 226567L, 226557L, 226558L, 227488L, 227489L)

  def getUrineInfo(icuStays: RDD[ICUStay], outputEvents: RDD[OutputEvent]): RDD[UrineOutputEvent] = {
    val o = outputEvents.filter(r => urineCodes.contains(r.itemID))
      .map(r => UrineOutputEvent(r.patientID, r.hadmID, r.icuStayID, r.chartTime, if (r.itemID == 227488) (-r.amount) else r.amount))

    o
  }
}
