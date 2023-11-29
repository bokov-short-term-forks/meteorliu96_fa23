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
                      
            
  
