reticulate::use_python('/Users/xingyu/miniforge3/bin/python')


import os
import pandas as pd
from io import BytesIO
import requests, zipfile

data_file = 'data_py' 


if not os.path.exists(os.path.join(data_file, 'mimic-iv-clinical-database-demo-1.0')  ):
  os.makedirs(data_file, exist_ok=True) 
  Input_Data = 'https://physionet.org/static/published-projects/mimic-iv-demo/mimic-iv-clinical-database-demo-1.0.zip';
  Zipped_Data = BytesIO(requests.get(Input_Data, stream=True).content)
  zipfile.ZipFile(Zipped_Data).extractall(data_file)
            
mimic = {filename.split('.')[0] : pd.read_csv(os.path.join(dirpath, filename))
            for dirpath, dirnames, filenames in os.walk(data_file)
            for filename in filenames
            if filename.endswith('.gz')}
                      
mimic['admissions'].iloc[:,:2].value_counts()  

democolumns = ['subject_id','insurance', 'marital_status', 'ethnicity']

def unique_concat(column):
  return ";".join(column.dropna().unique())


demographics = mimic['admissions'][democolumns + ['deathtime']].assign(
  ethnicity = mimic['admissions']['ethnicity'].replace('UNABLE TO OBTAIN', 'UNKNOWN'),
  deathtime = lambda df : pd.to_datetime(df['deathtime'])
  ).drop_duplicates().groupby('subject_id').aggregate( 
  { 'insurance': unique_concat, 'marital_status': unique_concat, 'ethnicity': unique_concat,
  'deathtime': max}
  ).assign(ethnicity = lambda df : df['ethnicity'].replace(['WHITE;UNKNOWN','UNKNOWN;WHITE'], 'WHITE'))

demographics['ethnicity'].value_counts()
 
named_outputevents = mimic['outputevents'].merge(mimic['d_items'], how = 'left')
named_labevents = mimic['labevents'].merge(mimic['d_labitems'], how = 'left')
named_chartevents = mimic['chartevents'].merge(mimic['d_items'], how = 'left')
named_inputevents = mimic['inputevents'].merge(mimic['d_items'], how = 'left')
named_icd = mimic['diagnoses_icd'].merge(mimic['d_icd_diagnoses'], how = 'left')

mimic['admissions']

"""
adm_scaffold <- admissions %>%
  transmute( hadm_id = hadm_id, subject_id = subject_id,
             los = ceiling(as.numeric(dischtime - admittime) / 24),
             date = purrr::map2(admittime,dischtime,
                                function(xx,yy){
                                  seq(trunc(xx,units = 'days'),yy, by = 'day');
                                  })
             ) %>% tidyr::unnest(date)
"""
mimic['admissions'][['hadm_id','subject_id','admittime','dischtime']].assign(
    admittime=lambda df: pd.to_datetime(df['admittime'], errors='coerce'),
    dischtime=lambda df: pd.to_datetime(df['dischtime'], errors='coerce'),
    los=lambda df: np.ceil((df['dischtime'] - df['admittime']).dt.total_seconds() / 86400),
    date=lambda df: [pd.date_range(start, end, freq='D') for start, end in zip(df['admittime'], df['dischtime'])]
).explode('date')

