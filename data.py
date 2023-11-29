reticulate::use_python('/Users/xingyu/miniforge3/bin/python')


import os
import pandas as pd
from io import BytesIO
import requests, zipfile

data_file = 'data_py' 

os.makedirs(data_file, exist_ok=True)           
Input_Data = 'https://physionet.org/static/published-projects/mimic-iv-demo/mimic-iv-clinical-database-demo-1.0.zip';
Zipped_Data = BytesIO(requests.get(Input_Data, stream=True).content)
zipfile.ZipFile(Zipped_Data).extractall(data_file)
            
mimic = {filename.split('.')[0] : pd.read_csv(os.path.join(dirpath, filename))
            for dirpath, dirnames, filenames in os.walk(data_file)
            for filename in filenames
            if filename.endswith('.gz') }
                      
            
  
  # #' # Import the data
  # Input_Data <- 'https://physionet.org/static/published-projects/mimic-iv-demo/mimic-iv-clinical-database-demo-1.0.zip';
  # dir.create('data',showWarnings = FALSE);
  # Zipped_Data <- file.path("data",'tempdata.zip');
  # download.file(Input_Data,destfile = Zipped_Data);
  # Unzipped_Data <- unzip(Zipped_Data,exdir = 'data') %>% grep('gz$',.,val=T);
  # Table_Names <- path_ext_remove(Unzipped_Data) %>% fs::path_ext_remove() %>% basename;
  # for(ii in seq_along(Unzipped_Data)) {
  #   assign(Table_Names[ii]
  #          ,import(Unzipped_Data[ii],format='csv')  %>% mutate(across(where(~is(.x,"IDate")), as.Date))
  # 
  #          )};
  # #mapply(function(aa,bb) assign(aa,import(bb,format='csv'),inherits = T),Table_Names,Unzipped_Data)
  # save(list=c(Table_Names,'Table_Names'),file='data.R.rdata');
  # print('Downloaded')

