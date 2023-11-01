-- !preview conn=DBI::dbConnect(RSQLite::SQLite())

SELECT table_name,STRING_AGG(column_name, ',') FROM ICU_Admissions_Data.INFORMATION_SCHEMA.COLUMNS
GROUP BY table_name;
--WHERE table_name = 'outputevents';

SET @@dataset_id = 'ICU_Admissions_Data';

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


