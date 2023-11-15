SELECT table_name,STRING_AGG(column_name, ',') FROM ICU_Admissions_Data.INFORMATION_SCHEMA.COLUMNS
GROUP BY table_name;
--WHERE table_name = 'outputevents';

SET @@dataset_id = 'ICU_Admissions_Data';
--CREATE OR REPLACE PROCEDURE prep_data()
--BEGIN
  /*
  democolumns = c('subject_id','insurance', 'marital_status', 'ethnicity')

  demographics = admissions %>% group_by(subject_id) %>%
    summarise(across(any_of(democolumns), unique_values)
              ,decease = any(!is.na(deathtime))
              ,deathtime = max(deathtime, na.rm = T)
                ) %>%
    mutate(ethnicity_revised = str_replace(ethnicity, 'UNKNOWN;', ''),
          ethnicity_revised_gsub = gsub('UNKNOWN;', '', ethnicity),
          )
  demographics[is.infinite(demographics$deathtime), "deathtime"] = NA
  demographics = demographics %>% left_join(patients[,c("subject_id", "gender","anchor_age")])

  */
  SET @@dataset_id = 'ICU_Admissions_Data';

  DROP TABLE IF EXISTS demographics;
  CREATE TABLE demographics as
  With demo as
  (SELECT subject_id,
    string_agg(DISTINCT insurance, "|") as insurance,
    string_agg(DISTINCT marital_status, "|") as marital_status,
    replace(replace(replace(string_agg(DISTINCT ethnicity, '|'), '|UNKNOWN', ''), 'UNKNOWN|', ''), '|OTHER','') as ethnicity,
    max(deathtime) as deathtime,
    max(CASE
      WHEN deathtime is not NULL THEN 1
      ELSE 0
    END) as decease
  FROM admissions
  GROUP BY subject_id)
  SELECT demo.*, gender, anchor_age FROM demo
  LEFT JOIN patients on demo.subject_id = patients.subject_id;

  /*
  named_labevents = labevents %>% left_join(d_labitems, by = c('itemid' = 'itemid'))
  named_chartevents = chartevents %>% left_join(d_items, by = c('itemid' = 'itemid'))
  named_inputevents = inputevents %>% left_join(d_items, by = c('itemid' = 'itemid'))
  named_icd = diagnoses_icd %>% left_join(d_icd_diagnoses)
  */

  DROP TABLE IF EXISTS named_outputevents;
  CREATE TABLE named_outputevents as
  SELECT d_items.*,subject_id,hadm_id,stay_id,charttime,storetime,value,valueuom
  FROM outputevents
  LEFT JOIN d_items on outputevents.itemid = d_items.itemid;

  DROP TABLE IF EXISTS named_labevents;
  CREATE TABLE named_labevents as
  SELECT d_labitems.*,labevent_id,subject_id,hadm_id,specimen_id,charttime,storetime,value,valuenum,valueuom,ref_range_lower,ref_range_upper,flag,priority,comments
  FROM labevents
  LEFT JOIN d_labitems on labevents.itemid = d_labitems.itemid;

  DROP TABLE IF EXISTS named_chartevents;
  CREATE TABLE named_chartevents as
  SELECT d_items.*,subject_id,hadm_id,stay_id,charttime,storetime,value,valuenum,valueuom,warning
  FROM chartevents
  LEFT JOIN d_items on chartevents.itemid = d_items.itemid;

  DROP TABLE IF EXISTS named_icd;
  CREATE TABLE named_icd as
  SELECT diagnoses_icd.*,long_title
  FROM diagnoses_icd
  LEFT JOIN d_icd_diagnoses on diagnoses_icd.icd_code = d_icd_diagnoses.icd_code;

  /*
  adm_scaffold = admissions %>% transmute( hadm_id = hadm_id, subject_id = subject_id,
                                      los = ceiling(as.numeric(dischtime - admittime) / 24),
                          date = purrr::map2(admittime,dischtime, function(xx,yy) seq(trunc(xx,units = 'days'),yy, by = 'day'))
                          ) %>% tidyr::unnest(date)

  */

  DROP TABLE IF EXISTS adm_scaffold;
  CREATE TABLE adm_scaffold as(
    WITH RECURSIVE q0 as(
      SELECT hadm_id,subject_id,Date(admittime) as date, Date(dischtime) as dischtime,
        date_diff(dischtime, admittime, day) as los
      FROM admissions
      UNION ALL
      SELECT hadm_id,subject_id,date_add(date, INTERVAL 1 day) as date, dischtime, los
      FROM q0
      WHERE date < dischtime
    )
    SELECT hadm_id,subject_id, date, los
    FROM q0
    ORDER BY hadm_id, date
  );

  DROP TABLE IF EXISTS ICU_scaffold;
  CREATE TABLE ICU_scaffold as(
    WITH RECURSIVE q0 as(
        SELECT hadm_id,subject_id, stay_id,  ceiling(los) as ICU_revised_los,
          Date(intime) as ICU_date, Date(outtime) as outtime,los as ICU_los, intime
        FROM icustays
        UNION ALL
        SELECT hadm_id,subject_id,stay_id, ICU_revised_los,
          date_add(ICU_date, INTERVAL 1 day) as ICU_date, outtime, ICU_los, intime
        FROM q0
        WHERE ICU_date < outtime
      ),
      q1 as (SELECT hadm_id,subject_id, stay_id, ICU_date, ICU_los, ICU_revised_los, intime, --new added
        ROW_NUMBER() OVER (PARTITION BY hadm_id, ICU_date order by intime) as rn
      FROM q0
      ORDER BY hadm_id, ICU_date
      )
      SELECT *
      FROM q1
      WHERE rn = 1
  );
/*
ICU_scaffold = icustays %>% transmute( hadm_id, subject_id , stay_id , ICU_los = los,
                                       ICU_los_revised = ceiling(as.numeric(outtime - intime) / 1440),
                                         ICU_date = purrr::map2(intime,outtime, function(xx,yy) seq(trunc(xx,units = 'days'),yy, by = 'day'))
) %>% tidyr::unnest(ICU_date) %>%
  group_by(hadm_id, subject_id , ICU_date) %>% summarise(stay_id = list(stay_id),
                                                         ICU_los = list(ICU_los))

*/
  SET @@dataset_id = 'ICU_Admissions_Data';
  DROP TABLE IF EXISTS main_data;
  CREATE TABLE main_data as(
    with q0 as(
      SELECT adm_scaffold.*, stay_ID, ICU_revised_los
      FROM adm_scaffold
      LEFT JOIN ICU_scaffold on adm_scaffold.hadm_id = ICU_scaffold.hadm_id and adm_scaffold.date = ICU_scaffold.ICU_date
    ),
    q1 as(
    SELECT subject_id, Date(charttime) as charttime, min(valuenum) AS pH, max(IF(flag = 'abnormal',1,0)) AS pH_flag
    FROM named_labevents
    WHERE itemid = 50820
    GROUP BY subject_id, Date(charttime)
    )
    SELECT q0.*, --IF(named_icd.hadm_id IS null, 0, 1) AS Hypoglycemia,
      IF(temp1.long_title IS null, 0, 1) AS Hypertension,
      pH, pH_flag
    FROM q0
    LEFT JOIN named_icd on q0.hadm_id = named_icd.hadm_id
      and icd_code IN ('E11649','E162', 'E161', 'E160', 'E13141', 'E15')
    LEFT JOIN named_icd as temp1 on q0.hadm_id = temp1.hadm_id
      and temp1.long_title LIKE "%Hyperten%"
    LEFT JOIN q1 on q0.subject_id = q1.subject_id and q0.date = q1.charttime
    ORDER BY q0.hadm_id, date
  );


/*
pH_table = named_labevents %>% mutate( charttime = as.Date(charttime)) %>%
  filter(itemid == 50820) %>%
  group_by(subject_id, charttime) %>% summarise(pH = min(valuenum),
                                                # flag = !any(between(valuenum, ref_range_lower, ref_range_upper)),
                                                pH_flag = any(flag=='abnormal')
                                                ) %>%  arrange(desc(pH))
*/
  SET @@dataset_id = 'ICU_Admissions_Data';

  --ORDER BY pH






--END