#'---
#' title: "Data Extraction"
#' author: 'Author One ^1^, Author Two ^1^'
#' abstract: |
#'  | Import data from an online source for use by the remaining scripts in this
#'  | project.
#' documentclass: article
#' description: 'Manuscript'
#' clean: false
#' self_contained: true
#' number_sections: false
#' keep_md: true
#' fig_caption: true
#' output:
#'  html_document:
#'    toc: true
#'    toc_float: true
#'    code_folding: show
#' ---
#'
#+ init, echo=FALSE, message=FALSE, warning=FALSE
# init ----
# This part does not show up in your rendered report, only in the script,
# because we are using regular comments instead of #' comments
debug <- 0;
upload_to_google <- 0;
knitr::opts_chunk$set(echo=debug>-1, warning=debug>0, message=debug>0);
refresh <- 0;

authemail <- 'meteor123sanctity@gmail.com';
projectid <- 'potent-bulwark-401719';
datasetid <- 'ICU_Admissions_Data';

# If you have a config_local.R, you can use it to override the default values above
if(file.exists('config_local.R')) source('config_local.R');

library(ggplot2); # visualisation
library(GGally);
library(rio);# simple command for importing and exporting data
library(pander); # format tables
library(printr); # automatically invoke pander when tables are detected
library(broom); # standardized, enhanced views of various objects
library(dplyr); # table manipulation
library(fs);    # file system operations
library(purrr); # package contains map2
library(tidyr); # package contains unnest
library(stringr); # string operations
library(googleAuthR); # interacting with Google BigQuery
library(bigQueryR);
library(DataExplorer);
library(explore);


options(max.print=42);
# defines what the rio and datatable packages treat as missing/NA in the input files
options(datatable.na.strings=c('NA','NULL',''));
options(datatable.integer64="numeric")
panderOptions('table.split.table',Inf); panderOptions('table.split.cells',Inf);


starting_names <- ls()

# download data
if(!file.exists('data.R.rdata')|refresh){
  #' # Import the data
  Input_Data <- 'https://physionet.org/static/published-projects/mimic-iv-demo/mimic-iv-clinical-database-demo-1.0.zip';
  dir.create('data',showWarnings = FALSE);
  Zipped_Data <- file.path("data",'tempdata.zip');
  download.file(Input_Data,destfile = Zipped_Data);
  Unzipped_Data <- unzip(Zipped_Data,exdir = 'data') %>% grep('gz$',.,val=T);
  Table_Names <- path_ext_remove(Unzipped_Data) %>% fs::path_ext_remove() %>% basename;
  for(ii in seq_along(Unzipped_Data)) {
    assign(Table_Names[ii]
           ,import(Unzipped_Data[ii],format='csv')  %>% mutate(across(where(~is(.x,"IDate")), as.Date))

           )};
  #mapply(function(aa,bb) assign(aa,import(bb,format='csv'),inherits = T),Table_Names,Unzipped_Data)
  save(list=c(Table_Names,'Table_Names'),file='data.R.rdata');
  print('Downloaded')
}else{
  print('File exists')
  load(file = "data.R.rdata")
  Table_Names <- setdiff(ls(),c(starting_names,'starting_names'))
}


democolumns <- c('subject_id','insurance', 'marital_status', 'ethnicity')

length_unique <- function(x) unique(x) %>% length()

unique_values <- function(x) unique(x) %>% sort() %>% paste(sep = '_',collapse = ':')

count_by_freq <- . %>% summarise(n=n(),patients=length_unique(subject_id)) %>% arrange(desc(n))

# vectorize running it by col
sapply(admissions[,democolumns], function(x) x %>% unique() %>% length())


admissions %>% group_by(subject_id) %>%  summarise(across(any_of(democolumns), length_unique))
#" # Demongraphic table

demographics <- admissions %>% group_by(subject_id) %>%
  summarise(across(any_of(democolumns), unique_values)
            ,decease = any(!is.na(deathtime))
            ,deathtime = max(deathtime, na.rm = T)
              ) %>%
  mutate(ethnicity_revised = str_replace(ethnicity, 'UNKNOWN;', ''),
         ethnicity_revised_gsub = gsub('UNKNOWN;', '', ethnicity),
         )

demographics[is.infinite(demographics$deathtime), "deathtime"] <- NA

demographics <- demographics %>% left_join(patients[,c("subject_id", "gender","anchor_age")])

# Commented-out example of getting a quick exploratory report of any dataset

# explore_shiny(demographics)
# explore::explore(name_outputevents)
# DataExplorer::create_report(name_outputevents)
# DataExplorer::create_report(demographics)
# table(demographics$decease)


# d_items %>% subset(linksto != "chartevents")
# d_items$linksto %>% table()
named_outputevents <- outputevents %>% left_join(d_items, by = c('itemid' = 'itemid'))

named_labevents <- labevents %>% left_join(d_labitems, by = c('itemid' = 'itemid'))
named_chartevents <- chartevents %>% left_join(d_items, by = c('itemid' = 'itemid'))
named_inputevents <- inputevents %>% left_join(d_items, by = c('itemid' = 'itemid'))
named_icd <- diagnoses_icd %>% left_join(d_icd_diagnoses)
# named_icd$long_title %>% table() %>% sort(decreasing = T) %>% t() %>% View()

# table(named_labevents$flag, named_labevents$label) %>% as.data.frame() %>% View()
# with(named_labevents,table(flag, label))  %>% as.data.frame() %>% View()

# lab/glucose, A1c, diagnosis/hypoglycemia,  death, icu stay, length of stay

admissions %>% group_by(subject_id) %>% summarise(ham = length(hadm_id))
admissions %>% select(subject_id, hadm_id, admittime, dischtime) %>% transmute()

# create a scaffold of admission dates
adm_scaffold <- admissions %>%
  transmute( hadm_id = hadm_id, subject_id = subject_id,
             los = ceiling(as.numeric(dischtime - admittime) / 24),
             date = purrr::map2(admittime,dischtime,
                                function(xx,yy){
                                  seq(trunc(xx,units = 'days'),yy, by = 'day');
                                  })
             ) %>% tidyr::unnest(date);

# # add stay_id (present if stay in ICU and NA if not) to scaffold


ICU_scaffold <- icustays %>%
  transmute( hadm_id, subject_id , stay_id , ICU_los = los,
             ICU_los_revised = ceiling(as.numeric(outtime - intime) / 1440),
             ICU_date = purrr::map2(intime,outtime, function(xx,yy){
               seq(trunc(xx,units = 'days'),yy, by = 'day')})
             ) %>% tidyr::unnest(ICU_date) %>%
  group_by(hadm_id, subject_id , ICU_date) %>%
  summarise(stay_id = list(stay_id),ICU_los = list(ICU_los));

htn_adm <- named_icd %>%
  subset(str_detect(tolower(long_title),'hypertension')) %>%
  pull(hadm_id) %>% unique();


p = c('E11649|E162');
hpgcm_adm <- named_icd %>% subset(str_detect(icd_code,p)) %>% pull(hadm_id) %>%
  unique();

# hpgcm_adm <- named_icd %>%  subset(str_detect(tolower(long_title),'hypertension')) %>%
#   pull(hadm_id) %>% unique()

main_data <- adm_scaffold %>%
  left_join(ICU_scaffold, by = c("hadm_id", "subject_id", 'date' = 'ICU_date')) %>%
  mutate(hypertention = subject_id %in% htn_adm,
         hypoglycemia = hadm_id %in% hpgcm_adm);

# named_labevents %>% group_by(category,fluid,loinc_code,label) %>%
#   summarize(n=n(),
#             patients=length_unique(subject_id)) %>%
#   arrange(desc(n)) %>% View()


pH_table <- named_labevents %>% mutate( charttime = as.Date(charttime)) %>%
  filter(itemid == 50820) %>%
  group_by(subject_id, charttime) %>%
  summarise(pH = min(valuenum),
            # flag = !any(between(valuenum, ref_range_lower, ref_range_upper)),
            pH_flag = any(flag=='abnormal')
            ) %>%  arrange(desc(pH));

# main_data$pH[is.na(main_data$pH)] = 7.4
vital_abrev<-c(HR="Heart Rate",aSBP="Arterial Blood Pressure systolic",
               mSBP="Manual Blood Pressure Systolic Left");


analytic_events <- named_chartevents %>%
  mutate( charttime = as.Date(charttime))  %>%
  group_by(label, subject_id, charttime) %>%
  filter(label %in% vital_abrev) %>%
  summarise(median_value = median(valuenum, na.rm = T)) %>%
  tidyr::pivot_wider(values_from = median_value, names_from = label ) %>%
  rename(vital_abrev);



main_data <- main_data %>%
  left_join(pH_table, by = c('subject_id', 'date' = 'charttime')) %>%
  left_join(analytic_events, by = c('subject_id', 'date' = 'charttime')) %>%
  left_join(demographics)


if(upload_to_google){
  # Authorization
  googleAuthR::gar_cache_empty();
  # Here I recommend you copy the OAuth .json file to this project folder and
  # name it Service_Account_SQL.json so that it is one less thing to configure
  # on other computers by people trying to replicate these results
  # in this case, me :-)
  googleAuthR::gar_set_client('Service_Account_SQL.json');
  # googleAuthR::gar_set_client("/Users/xingyu/Desktop/FA22TSCI5230/client_secret_959833717950-rk30n9msv2fdpllf174mtgpkphc5on8r.apps.googleusercontent.com.json")
  # Add testing user with email address in google `OA consent screen` first
  bqr_auth(email = authemail);

  #bqr_upload_data(projectid, datasetid, Table_Names[2], get(Table_Names[2]))

  for (ii in seq_along(Table_Names)) {
    bqr_upload_data(projectid, datasetid, Table_Names[ii], get(Table_Names[ii]));
  }
}
