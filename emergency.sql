# Extracts emergency ICU patient profiling
# The Query executed with Google Big Query platform

with main_table as
(select * from (select * from (
    SELECT ie.subject_id,ie.stay_id,
    adm.marital_status,adm.insurance,adm.admission_type,
CASE
    WHEN PAT.GENDER = 'M' then 1
    ELSE 0 END AS is_male,
CASE
    WHEN SERV.curr_service like '%SURG%' then 'SURG'
    ELSE 'other' END
  as surgical
 , DATETIME_DIFF(adm.admittime, DATETIME(pat.anchor_year, 1, 1, 0, 0, 0), YEAR) + pat.anchor_age as admission_age
, adm.hospital_expire_flag,
ROUND(DATETIME_DIFF(ie.outtime, ie.intime, HOUR)/24.0) as los_icu
, CASE
    WHEN DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) = 1 THEN 1
    ELSE 0 END AS is_first_icu_stay
 
FROM `physionet-data.mimic_icu.icustays` ie
INNER JOIN `physionet-data.mimic_core.admissions` adm
    ON ie.hadm_id = adm.hadm_id and ie.los<=30
INNER JOIN `physionet-data.mimic_core.patients` pat
    ON ie.subject_id = pat.subject_id and pat.anchor_age>=21 and pat.anchor_age<70
LEFT OUTER JOIN `physionet-data.mimic_hosp.services` serv
    on ie.hadm_id = serv.hadm_id  where adm.admission_type like '%EMER%') 
PIVOT
(
count(surgical) as service_type
  FOR surgical in ('SURG','other')
) SERV_TYPE ) k )
,
height as 
(SELECT
    ie.subject_id
    , ie.stay_id
    , ROUND(AVG(height), 2) AS height
FROM `physionet-data.mimic_icu.icustays` ie
LEFT JOIN `physionet-data.mimic_derived.height` ht
    ON ie.stay_id = ht.stay_id
    AND ht.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND ht.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id order by ie.subject_id),


first_day_vital_signs as
(SELECT
ie.subject_id
, ie.stay_id
, AVG(heart_rate) AS heart_rate_mean
, AVG(sbp) AS sbp_mean
, AVG(dbp) AS dbp_mean
, AVG(mbp) AS mbp_mean
, AVG(resp_rate) AS resp_rate_mean
, AVG(temperature) AS temperature_mean
, AVG(spo2) AS spo2_mean
, AVG(glucose) AS glucose_mean
FROM `physionet-data.mimic_icu.icustays` ie
LEFT JOIN `physionet-data.mimic_derived.vitalsign` ce
    ON ie.stay_id = ce.stay_id
    AND ce.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND ce.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id),


first_day_weight as 
(SELECT
  ie.subject_id
  , ie.stay_id
  , AVG(CASE WHEN weight_type = 'admit' THEN ce.weight ELSE NULL END) AS weight_admit
  , AVG(ce.weight) AS weight
  , MIN(ce.weight) AS weight_min
  , MAX(ce.weight) AS weight_max
FROM `physionet-data.mimic_icu.icustays` ie
  -- admission weight
LEFT JOIN `physionet-data.mimic_derived.weight_durations` ce
    ON ie.stay_id = ce.stay_id
    -- we filter to weights documented during or before the 1st day
    AND ce.starttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id)



select mt.marital_status,mt.insurance,mt.admission_type,mt.is_male,mt.admission_age,
mt.los_icu,mt.is_first_icu_stay,mt.service_type_SURG,round(he.height) height,round(weigth.weight) as weight, round(((weigth.weight*100) /he.height)) as bmi,
round(vs.heart_rate_mean) as heart_rate_mean,
round(vs.sbp_mean) as sbp_mean, round(vs.dbp_mean) as dbp_mean,round(vs.resp_rate_mean ) as resp_rate_mean,
round(vs.temperature_mean) as body_temperature_mean, round(vs.spo2_mean ) as spo2_mean ,
round(vs.glucose_mean) as glucose_mean,
mt.hospital_expire_flag from main_table mt join height he
on mt.subject_id=he.subject_id and mt.stay_id=he.stay_id join
first_day_vital_signs  vs on vs.subject_id=mt.subject_id and vs.stay_id=mt.stay_id join 
first_day_weight weigth on weigth.subject_id=mt.subject_id and weigth.stay_id=mt.stay_id where he.height is not null and weigth.weight
is not null
