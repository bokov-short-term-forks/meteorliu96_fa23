SET @@dataset_id = 'ICU_Admissions_Data';
/*
counts_dups = function(tablename, columnnames, selectname, dataset){
  attach(mydataset)
  sqlquery = paste0('SELECT ', columnnames, ' FROM ', mydataset,'.', tablename)
  ** psedu code
}
counts_dups('ICU_scaffold', 'hadm_id, stay_id', NULL, NULL)
*/
CREATE OR REPLACE PROCEDURE count_dups(
  tablename STRING, columnnames STRING, selectname STRING, dataset STRING
)
BEGIN
  DECLARE mydataset STRING;
  DECLARE sqlquery STRING;

  SET mydataset = (SELECT coalesce(dataset, 'ICU_Admissions_Data'));
  SET sqlquery = concat('SELECT ', columnnames, ' FROM ', mydataset,'.', tablename);
  SELECT sqlquery;
  EXECUTE IMMEDIATE sqlquery;

END;

CALL ICU_Admissions_Data.count_dups('ICU_scaffold', 'hadm_id, stay_id', NULL, NULL)
