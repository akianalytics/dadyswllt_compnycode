
############R libraries .
library(lubridate)
library(dplyr)
library(reshape2)
library(aws.s3)
library(RJSONIO)
#####################################################################
#### Setting S3 environment .######################################################
Sys.setenv("AWS_ACCESS_KEY_ID" ='AKIAIVYJXYSVCXM52T4Q',  
           "AWS_SECRET_ACCESS_KEY" = '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',
           "AWS_DEFAULT_REGION" = "ap-southeast-1"
)
################################################################################

##################Function to  fetch data from Amazon S3 database .

aws_call<-function(userid){
  
  res<-get_object(userid, bucket = "wallet1234")
  p<-rawToChar(res)
  l<-fromJSON(p)
  k<-l$data
  call_details<-fromJSON(k, simplifyVector = FALSE)
  rel<-data.frame(do.call("rbind",call_details))
  dat <- lapply(call_details, function(j) {
    as.data.frame(replace(j, sapply(j, is.list), NA))
  })
  df_all <- rbind.fill(dat)
  data.frame(df_all=df_all)
  return(df_all)
}



###############################################Get  bucket details ################
files <- get_bucket(bucket = 'wallet1234') 
##############################################################################

###########################################################
rel<-data.frame(do.call("rbind",files))
dat <- lapply(files, function(j) {
  list(replace(j, sapply(j, is.list), NA))
})

key_values<-data.frame(rel$Key)
colnames(key_values) <- as.character(unlist(key_values[1,]))
key_values=key_values[-1,]
call_S3<-data<-subset(key_values, select = grepl("sms/", names(key_values))) ## Took out  all userid having call.
call_S3_character<-names(call_S3) ### userids .
####################################################################################################

####taking out  userids .

id<- gsub("call/","",call_S3_character)

############################

## 1. all  calls analysis.
fun<-function(tr){
  
  tryCatch({
  df<-aws_call(call_S3_character[tr])
  
  # df<-users11_calls
  df$date<-if( c("date")%in%names(df)=="FALSE")
    
  {
    df$date<-0
    
  }else{
    df$date<-df$date
  }
  
  df$normalized_number<-if( c("normalized_number")%in%names(df)=="FALSE")
    
  {
    df$normalized_number<-0
    
  }else{
    df$normalized_number<-df$normalized_number
  }
  
  df$type<-if( c("type")%in%names(df)=="FALSE")
    
  {
    df$type<-0
    
  }else{
    df$type<-df$type
  }
  
  
  df$numbertype<-if( c("numbertype")%in%names(df)=="FALSE")
    
  {
    df$numbertype<-0
    
  }else{
    df$numbertype<-df$numbertype
  }
  
  
  df$name<-if( c("name")%in%names(df)=="FALSE")
    
  {
    df$name<-0
    
  }else{
    df$name<-df$name
  }
  
  df_date_parsed<-tbl_df(df) 
  
  ######################Changing  date(epoc format ) to  date & time format . 
  df_date_time<-as.POSIXct((df$date)/1000,origin="1970-01-01")
  df_date<-substr(df_date_time,1,11)
  df_time<-substr(df_date_time,11,20)
  df_date_parsed<-cbind(df,df_date,df_time)
  
  ########## taking out laast 10  digits of normalized number column .
  df_date_parsed$normalized_number<-as.character(df_date_parsed$normalized_number)
  df_date_parsed$normalized_number<-substr(df_date_parsed$normalized_number,nchar(df_date_parsed$normalized_number)-9,nchar(df_date_parsed$normalized_number))
  
  ####################################################################################
  ############Analysisng a persons phone is new/old(If the person is having less than 90 days data ,he i having new phone .)
  New_device<-length(unique(df_date_parsed$df_date))
  ######################################################################################
  
  ###########Incoming calls  .
  #tryCatch({
  incoming_calls<- df_date_parsed%>%subset(type==1) ## Taking out all  incoming calls.
  incoming_calls_in_a_day<-incoming_calls%>%select(df_date,normalized_number)
  incoming_calls_a_day_unique<-by(as.numeric(incoming_calls_in_a_day$normalized_number),incoming_calls_in_a_day$df_date,unique)
  number_of_incoming_calls_recieved_per_day<-data.frame(sapply(incoming_calls_a_day_unique,length))  ## 
  average_of_incoming_calls<-round(sum(number_of_incoming_calls_recieved_per_day)/nrow(number_of_incoming_calls_recieved_per_day))
  #}, error=function(e){})
  
  ###########outgoing calls  .
  
  outgoing_calls<-df_date_parsed%>%subset(type==2) ## Taking out all  outgoing calls.
  outgoing_calls_in_a_day<-outgoing_calls%>%select(df_date,normalized_number)
  outgoing_calls_a_day_unique<-by(as.numeric(outgoing_calls_in_a_day$normalized_number),outgoing_calls_in_a_day$df_date,unique)
  number_of_outgoing_calls_recieved_per_day<-data.frame(sapply(outgoing_calls_a_day_unique,length))  ## 
  
  average_of_outgoing_calls<-round(sum(number_of_outgoing_calls_recieved_per_day)/nrow(number_of_outgoing_calls_recieved_per_day))
  
  
  ###########missed calls  .
  
  
  missed_calls<-df_date_parsed%>%subset(type==3) ## Taking out all  missed calls.
  missed_calls_in_a_day<-missed_calls%>%select(df_date,normalized_number)
  missed_calls_a_day_unique<-by(as.numeric(missed_calls_in_a_day$normalized_number),missed_calls_in_a_day$df_date,unique)
  number_of_missed_calls_recieved_per_day<-data.frame(sapply(missed_calls_a_day_unique,length))  ## 
  
  
  average_of_missed_calls<-round(sum(number_of_missed_calls_recieved_per_day)/nrow(number_of_missed_calls_recieved_per_day))
  
  
  
  ########################### Classification  based on call  recieved from work/home/etc.
  
  ##### How many  calls  user recieved from work .
  
  
  work_call<-df_date_parsed%>%subset(numbertype==3)
  work_call_in_a_day<-work_call%>%select(df_date,normalized_number)
  work_call_a_day_unique<-by(as.numeric(work_call_in_a_day$normalized_number),work_call_in_a_day$df_date,unique)
  number_of_work_call_recieved_per_day<-nrow(data.frame(sapply(work_call_a_day_unique,length)) ) ## 
  average_of_work_calls<-round(sum(number_of_work_call_recieved_per_day)/nrow(number_of_work_call_recieved_per_day))
  
  
  ##### How many  calls  user recieved from home.
  
  home_call<-df_date_parsed%>%subset(numbertype==1)
  home_call_in_a_day<-home_call%>%select(df_date,normalized_number)
  home_call_a_day_unique<-by(as.numeric(home_call_in_a_day$normalized_number),home_call_in_a_day$df_date,unique)
  number_of_home_call_recieved_per_day<-nrow(data.frame(sapply(home_call_a_day_unique,length)) ) ## 
  
  average_of_home_calls<-round(sum(number_of_home_call_recieved_per_day)/nrow(number_of_home_call_recieved_per_day))
  
  
  
  
  ##### How many calls  user recieved  as  mobile .
  
  mobile_call<-df_date_parsed%>% subset(numbertype==2)
  mobile_call_in_a_day<-mobile_call%>%select(df_date,normalized_number)
  mobile_call_a_day_unique<-by(as.numeric(mobile_call_in_a_day$normalized_number),mobile_call_in_a_day$df_date,unique)
  number_of_mobile_call_recieved_per_day<-data.frame(sapply(mobile_call_a_day_unique,length))  ## 
  
  average_of_mobile_calls<-round(sum(number_of_mobile_call_recieved_per_day)/nrow(number_of_mobile_call_recieved_per_day))
  
  
  
  ###  How many calls user recieved form workfax/homefax/custom .
  
  others_call<-df_date_parsed %>% subset(numbertype== 0 | numbertype == 7 | numbertype == 4 | numbertype ==5 | numbertype == 12)
  others_call_in_a_day<-others_call%>%select(df_date,normalized_number)
  others_call_a_day_unique<-by(as.numeric(others_call_in_a_day$normalized_number),others_call_in_a_day$df_date,unique)
  number_of_others_call_recieved_per_day<-data.frame(sapply(others_call_a_day_unique,length))  ## 
  
  average_of_others_calls<-round(sum(number_of_others_call_recieved_per_day)/nrow(number_of_others_call_recieved_per_day))
  
  
  ############################################################################################################################
  
  
  ######################Assigning points  to  sections .
  
  #### Incoming calls  points .
  
  average_of_incoming_calls<-data.frame(average_of_incoming_calls)
  
  average_incoming_call_points<-0
  
  if(average_of_incoming_calls > 15)
  {
    average_incoming_call_points<-3
  } else if(average_of_incoming_calls>8 && average_of_incoming_calls<14 ){
    
    average_incoming_call_points<-2
  } else  if(average_of_incoming_calls>4 && average_of_incoming_calls<8){
    
    average_incoming_call_points<-1 
  }else {
    
    average_incoming_call_points<-0
  }
  
  
  Avg_inc_call_day <-average_incoming_call_points
  
  
  ###################################################################################################################
  
  #### Outgoing calls  points .
  
  average_of_outgoing_calls<-data.frame(average_of_outgoing_calls)
  
  saved_outgoing_call_points<-0
  if(average_of_outgoing_calls> 15)
  {
    saved_outgoing_call_points<-3
  }else if(average_of_outgoing_calls>8 &&average_of_outgoing_calls<14 ){
    
    saved_outgoing_call_points<-2
  } else  if(average_of_outgoing_calls>4 &&average_of_outgoing_calls<8){
    
    saved_outgoing_call_points<-1 
  }else {
    
    saved_outgoing_call_points<-0
  }
  
  
  Avg_dia_call_day<-saved_outgoing_call_points
  
  
  #############################################################################################################
  
  ##### Missed calls count .
  
  average_of_missed_calls<-data.frame(average_of_missed_calls)
  
  saved_missed_call_points<-0
  
  if(average_of_missed_calls> 15)
  {
    saved_missed_call_points<-0
  }else if(average_of_missed_calls>8 && average_of_missed_calls<14 ){
    
    saved_missed_call_points<-1
  }else  if(average_of_missed_calls>4 &&average_of_missed_calls<8){
    
    saved_missed_call_points<-2
  } else {
    
    saved_missed_call_points<-3
  }
  
  
  
  Avg_mis_call_day<-saved_missed_call_points
  
  
  ###################################################################################################################
  average_of_work_calls<-data.frame(average_of_work_calls)
  Work_based_call<-0
  tryCatch({
    work_call_points<-0
    
    if(average_of_work_calls> 15)
    {
      work_call_points<-0
    }else if(average_of_work_calls>8 && average_of_work_calls<14 ){
      
      work_call_points<-1
    }else  if(average_of_work_calls>4 &&average_of_work_calls<8){
      
      work_call_points<-2
    } else {
      
      work_call_points<-3
    }
    
  }, error=function(e){})
  
  Work_based_call<-work_call_points
  
  
  
  ###################################################################################################################
  average_of_home_calls<-data.frame(average_of_home_calls)
  Home_based_call<-0
  tryCatch({
    home_call_points<-0
    
    if(average_of_home_calls> 15)
    {
      home_call_points<-0
    }else if(average_of_home_calls>8 && average_of_home_calls<14 ){
      
      home_call_points<-1
    }else  if(average_of_home_calls>4 &&average_of_home_calls<8){
      
      home_call_points<-2
    } else {
      
      home_call_points<-3
    }
    
    
    Home_based_call<-home_call_points
  }, error=function(e){})
  
  
  ##############################################################################################
  average_of_mobile_calls<-data.frame(average_of_mobile_calls)
  tryCatch({
    Mobile_based_call<-0
    
    mobile_call_points<-0
    
    if(average_of_mobile_calls> 15)
    {
      mobile_call_points<-0
    }else if(average_of_mobile_calls>8 && average_of_mobile_calls<14 ){
      
      mobile_call_points<-1
    }else  if(average_of_mobile_calls>4 &&average_of_mobile_calls<8){
      
      mobile_call_points<-2
    } else {
      
      mobile_call_points<-3
    }
    
    
    
    Mobile_based_call<-mobile_call_points
    
    
  }, error=function(e){})
  
  ##############################################################################################
  tryCatch({
    average_of_others_calls<-data.frame(average_of_others_calls)
    
    Other_based_call<-0
    
    others_calls_points<-0
    
    if(average_of_others_calls> 15)
    {
      others_calls_points<-0
    }else if(average_of_others_calls>8 && average_of_others_calls<14 ){
      
      others_calls_points<-1
    }else  if(average_of_others_calls>4 &&average_of_others_calls<8){
      
      others_calls_points<-2
    } else {
      
      others_calls_points<-3
    }
    
    
    
    Other_based_call<- others_calls_points
    
  }, error=function(e){})
  ######################################################################################################
  user<<-0
  
 
  user<<-data.frame(paste("user",tr,sep="_"),New_device,Avg_inc_call_day ,Avg_dia_call_day,Avg_mis_call_day ,
                   Work_based_call,Home_based_call,Mobile_based_call,Other_based_call) 
 

  }, error=function(e){
    
    user<<-data.frame(paste("user",tr,sep="_"),New_device=0,Avg_inc_call_day=0 ,Avg_dia_call_day=0,Avg_mis_call_day=0 ,
                      Work_based_call=0,Home_based_call=0,Mobile_based_call=0,Other_based_call=0) 
    
    
  })
  return(user)
}

y_call<-data.frame()
y_call<-do.call(rbind.data.frame,lapply(seq(call_S3_character),function(k){fun(k) }))
colnames(y_call)[1]<-"users"

########################################################################################
## 2 .saved/unsaved calls .
module_two<-function(pr){
  
  tryCatch({
  df<-aws_call(call_S3_character[pr])
  
  #df<-users1_calls
  
  ### If  the particular point is unavailable , we will assign  it as zero.
  
  #df$date<-ifelse((c("date")%in%names(df)=="FALSE"),0,df$date)
  
  df$date<-if( c("date")%in%names(df)=="FALSE")
    
  {
    df$date<-0
    
  }else{
    df$date<-df$date
  }
  
  #df$normalized_number<-ifelse((c("normalized_number")%in%names(df)=="FALSE"),0,df$normalized_number)
  df$normalized_number<-if( c("normalized_number")%in%names(df)=="FALSE")
    
  {
    df$normalized_number<-0
    
  }else{
    df$normalized_number<-df$normalized_number
  }
  
  df$type<-if( c("type")%in%names(df)=="FALSE")
    
  {
    df$type<-0
    
  }else{
    df$type<-df$type
  }
  
  
  #df$name<-ifelse((c("name")%in%names(df)=="FALSE"),0,df$name)
  df$name<-if( c("name")%in%names(df)=="FALSE")
    
  {
    df$name<-0
    
  }else{
    df$name<-df$name
  }
  
  
  
  #df$name<-ifelse((c("df$suggest_text_1")%in%names(df)=="FALSE"),0,df$df$suggest_text_1)
  df_date_parsed<-tbl_df(df) 
  ######################Changing  date(epoc format ) to  date & time format . 
  df_date_time<-as.POSIXct((df$date)/1000,origin="1970-01-01")
  df_date<-substr(df_date_time,1,11)
  df_time<-substr(df_date_time,11,20)
  df_date_parsed<-cbind(df,df_date,df_time)
  
  ########## taking out laast 10  digits of normalized number column .
  df_date_parsed$normalized_number<-as.character(df_date_parsed$normalized_number)
  df_date_parsed$normalized_number<-substr(df_date_parsed$normalized_number,nchar(df_date_parsed$normalized_number)-9,nchar(df_date_parsed$normalized_number))
  
  #########################################################################################################
  ##################Day analysis (Number of persons called from saved number /unsaved number )
  
  ###incoming,outgoing,missed .
  res_unique_day<-by(as.numeric(df_date_parsed$normalized_number),df_date_parsed$df_date,unique)  ##Unique number calls he got or called .
  res_unique_day_count<-data.frame(sapply(res_unique_day,length))  ## Number of unique number calls he got or called  .
  
  #####saved contacts details calls done or recieved  .
  unique_day_data<-df_date_parsed %>% select(name,df_date)
  
  ######SAVED  NUMBERS (ALL)#################
  saved_unique_day_data <-na.omit(unique_day_data)
  saved_unique_day_data_count<-by(saved_unique_day_data$name,saved_unique_day_data$df_date,unique)
  saved_count_calls<-data.frame(sapply(saved_unique_day_data_count,length)) ##number of  calls done/recieved on a daily  basis from saved numbers  .
  #########################################
  
  #######UNSAVED NUMBER (ALL) ############
  unsaved_unique_day_data<-unique_day_data[is.na(unique_day_data),]
  #unsaved_unique_day_data_count<-by(unsaved_unique_day_data$name,unsaved_unique_day_data$df_date,unique)
  #unsaved_count_calls<-data.frame(sapply(unsaved_unique_day_data_count,length)) ##number of  calls done/recieved on a daily  basis from  unsaved numbers .
  unsaved_count_calls<-length(unsaved_unique_day_data[,2] )
  
  ########################################
  
  
  incoming_calls_unique <-df_date_parsed%>%subset(type==1) ## Taking out all  incoming calls.
  res_unique_day_incoming<-by(incoming_calls_unique$normalized_number,incoming_calls_unique$df_date,unique)  ##Unique number calls  of incoming .
  res_unique_day_incoming_count<-data.frame(sapply(res_unique_day_incoming,length))  ## Number of unique calls  he got in incoming .
  
  ##### contacts details calls  recieved  .
  Incoming_unique_day_data<-incoming_calls_unique%>% select(name,df_date)
  
  
  ######SAVED  NUMBERS (INCOMING)#################
  saved_unique_incoming_day_data <-na.omit(Incoming_unique_day_data)
  saved_unique_incoming_day_data_count<-by(saved_unique_incoming_day_data$name,saved_unique_incoming_day_data$df_date,unique)
  #saved_incoming_count_calls<-data.frame(unlist(saved_unique_incoming_day_data_count)) ##number of  calls done/recieved on a daily  basis from saved numbers  .
  saved_incoming_count_calls<-data.frame(sapply(saved_unique_incoming_day_data_count,length))
  
  average_saved_incoming<-round(sum(saved_incoming_count_calls)/nrow(saved_incoming_count_calls)) 
  
  
  #########################################
  
  #######UNSAVED NUMBER (INCOMING) ############
  
  unsaved_unique_incoming_day_data<-Incoming_unique_day_data[is.na(Incoming_unique_day_data),]
  #unsaved_unique_day_incoming_data_count<-by(matrix(unsaved_unique_incoming_day_data$name),as.Date(unsaved_unique_incoming_day_data$df_date),count)
  unsaved_unique_day_incoming_data_count<-ifelse(nrow(unsaved_unique_incoming_day_data)==0,0,unsaved_unique_day_incoming_data_count)
  unsaved_incoming_count_calls<-data.frame(unlist(unsaved_unique_day_incoming_data_count)) ##number of  calls done/recieved on a daily  basis from  unsaved numbers .
  #unsaved_incoming_count_calls<-length(tryCatch ({ unsaved_unique_incoming_day_data[,2] }, error=function(e){}))
  average_unsaved_incoming<-round(sum(unsaved_incoming_count_calls)/nrow(unsaved_incoming_count_calls))
  
  
  ########################################
  
  outgoing_calls_unique<-df_date_parsed%>%subset(type==2) ## Taking out all  out  calls.
  res_unique_day_outgoing<-by(outgoing_calls_unique$normalized_number,outgoing_calls_unique$df_date,unique)
  res_unique_day_outgoing_count<-data.frame(sapply(res_unique_day_outgoing,length))
  
  ##### contacts details calls done  .
  outgoing_unique_day_data<-outgoing_calls_unique%>% select(name,df_date)
  
  
  ######SAVED  NUMBERS (OUTGOING)#################
  
  saved_unique_outgoing_day_data <-na.omit(outgoing_unique_day_data)
  saved_unique_outgoing_day_data_count<-by(saved_unique_outgoing_day_data$name,saved_unique_outgoing_day_data$df_date,unique)
  #saved_outgoing_count_calls<-data.frame(unlist(saved_unique_outgoing_day_data_count)) ##number of  calls done/recieved on a daily  basis from saved numbers  .
  saved_outgoing_count_calls<-data.frame(sapply(saved_unique_outgoing_day_data_count,length))
  average_saved_outgoing<-round(sum(saved_outgoing_count_calls)/nrow(saved_outgoing_count_calls)) 
  
  
  #########################################
  
  #######UNSAVED NUMBER (OUTGOING) ############
  
  unsaved_unique_outgoing_day_data<-outgoing_unique_day_data[is.na(outgoing_unique_day_data),]
  unsaved_unique_day_outgoing_data_count<-ifelse(nrow(unsaved_unique_outgoing_day_data)==0,0,by(matrix(unsaved_unique_outgoing_day_data$name),as.Date(unsaved_unique_outgoing_day_data$df_date),count))
  unsaved_outgoing_count_calls<-data.frame(unlist(unsaved_unique_day_outgoing_data_count)) ##number of  calls done/recieved on a daily  basis from  unsaved numbers .
  
  #unsaved_outgoing_count_calls<-count(tryCatch ({unsaved_unique_outgoing_day_data}, error=function(e){}))
  average_unsaved_outgoing<-round(sum(unsaved_outgoing_count_calls)/nrow(unsaved_outgoing_count_calls))
  
  
  ########################################
  
  missed_calls_unique<-df_date_parsed%>%subset(type==3)
  res_unique_day_missed<-by(missed_calls_unique$normalized_number,missed_calls_unique$df_date,unique)
  res_unique_day_missed_count<-data.frame(sapply(res_unique_day_missed,length))
  
  ##### Missed calls  from saved and unsaved contacts .
  missed_unique_day_data<-missed_calls_unique%>% select(name,df_date)
  
  
  ######SAVED  NUMBERS (MISSED CALLS)#################
  
  saved_unique_missed_day_data <-na.omit(missed_unique_day_data)
  saved_unique_missed_day_data_count<-by(saved_unique_missed_day_data$name,saved_unique_missed_day_data$df_date,unique)
  #saved_missed_count_calls<-data.frame(unlist(saved_unique_outgoing_day_data_count)) ##number of  calls done/recieved on a daily  basis from saved numbers  .
  
  saved_missed_count_calls<-data.frame(sapply(saved_unique_missed_day_data_count,length))
  #row.names(saved_unique_outgoing_day_data_count)<-NULL
  average_saved_missedcall<-round(sum(saved_missed_count_calls)/nrow(saved_missed_count_calls))
  
  
  #########################################
  
  #######UNSAVED NUMBER (MISSED CALLS) ############
  
  unsaved_unique_missed_day_data<-missed_unique_day_data[is.na(missed_unique_day_data),]
  unsaved_unique_day_missed_data_count<-ifelse(nrow(unsaved_unique_missed_day_data)==0,0,by(matrix(unsaved_unique_missed_day_data$name),as.Date(unsaved_unique_missed_day_data$df_date),count))
  unsaved_missed_count_calls<-data.frame(unlist(unsaved_unique_day_missed_data_count)) ##number of  calls done/recieved on a daily  basis from  unsaved numbers .
  #unsaved_missed_count_calls<-count(tryCatch ({unsaved_unique_missed_day_data[2] }, error=function(e){}))
  average_unsaved_missedcall<-round(sum(unsaved_missed_count_calls)/nrow(unsaved_missed_count_calls))
  
  
  
  ########################################
  
  
  ##################### Assigning  points to the  results recieved .
  
  ### Calls  recieved/done  from  saved contacts per day .
  
  saved_count_calls<-saved_count_calls
  
  
  ###################################### INCOMING SAVED  ##########################################################
  
  ### Calls  recieved from  saved contacts per day(  >15 :-- 3  ,8-14 :--2    ,4- 8 :--1 , else  0)  .
  
  average_saved_incoming
  saved_incoming_points<-0
  
  tryCatch({
    
    if(average_saved_incoming>=15){
      saved_incoming_points<-3
    } else if(average_saved_incoming>8 && average_saved_incoming<14){
      saved_incoming_points<-2
    }else if(average_saved_incoming>4 && average_saved_incoming<7){
      saved_incoming_points<-1
    }else{
      saved_incoming_points<-0
    }
    
    
  },error=function(e){})
  
  Avg_Call_rev_saved_cont<-saved_incoming_points
  
  
  
  # saved_incoming_count_calls<-data.frame(saved_incoming_count_calls)
  # 
  # saved_incoming_points<-0
  # for(i in 1:nrow(saved_incoming_count_calls)){
  # tryCatch({
  #   if(saved_incoming_count_calls[i,1]> 15)
  # {
  #   saved_incoming_points[i]<-3
  # }
  #   else if(saved_incoming_count_calls[i,1]>8 && saved_incoming_count_calls[i,1]<14 ){
  #   
  #   saved_incoming_points[i]<-2
  # }
  # else  if(saved_incoming_count_calls[i,1]>4 && saved_incoming_count_calls[i,1]<8){
  #   
  #   saved_incoming_points[i]<-1 
  # }
  # 
  # else {
  #   
  #   saved_incoming_points[i]<-0
  # }
  # 
  # }, error=function(e){})
  # }
  
  #saved_incoming_calls_points_result<-cbind (saved_incoming_count_calls,saved_incoming_points)
  
  #############################################################################################################
  
  #####################################INCOMING UNSAVED ########################################################
  
  ### Calls  recieved from  unsaved contacts per day(> 15  :-- 0 , 8-14 :-- 1 ,4-8 :--2  ,else 3) .
  
  average_unsaved_incoming
  unsaved_incoming_points<-0
  
  tryCatch({
    
    if(average_unsaved_incoming>=15){
      unsaved_incoming_points<-0
    } else if(average_unsaved_incoming>8 && average_unsaved_incoming<14){
      unsaved_incoming_points<-1
    }else if(average_unsaved_incoming>4 && average_unsaved_incoming<7){
      unsaved_incoming_points<-2
    }else{
      unsaved_incoming_points<-3
    }
    
    
  },error=function(e){})
  
  Avg_Call_rev_unsaved_cont<-unsaved_incoming_points
  
  # unsaved_incoming_count_calls_all<-unsaved_incoming_count_calls
  # unsaved_incoming_count_calls<-data.frame(tryCatch({unsaved_incoming_count_calls[2]}, error=function(e){}))
  # unsaved_incoming_points<-0
  # for(i in 1:nrow(unsaved_incoming_count_calls)){
  #   tryCatch({
  #   if(unsaved_incoming_count_calls[i,1]> 15)
  #   {
  #     unsaved_incoming_points[i]<-0
  #   }
  #   else if(unsaved_incoming_count_calls[i,1]>8 && unsaved_incoming_count_calls[i,1]<14 ){
  #     
  #     unsaved_incoming_points[i]<-1
  #   }
  #   else  if(unsaved_incoming_count_calls[i,1]>4 && unsaved_incoming_count_calls[i,1]<8){
  #     
  #     unsaved_incoming_points[i]<-2
  #   }
  #   
  #   else {
  #     unsaved_incoming_points[i]<-3
  #     
  #   }
  #  
  #   }, error=function(e){})
  # }
  # 
  # 
  # unsaved_in coming_calls_points_result<-tryCatch({
  # unsaved_incoming_calls_points_result<-cbind (unsaved_incoming_count_calls_all,unsaved_incoming_points)
  # }, error=function(e){})
  # 
  
  ###############################################################################################################
  
  ######################################OUTGOING SAVED ################################################
  ###  Calls outgoing   to  saved contacts  per day (> 15  :-- 0 , 8-14 :-- 1 ,4-8 :--2  ,else 3) ..
  
  
  average_saved_outgoing
  
  saved_outgoing_points<-0
  
  tryCatch({
    
    if(average_saved_outgoing>=15){
      saved_outgoing_points<-3
    } else if(average_saved_outgoing>8 && average_saved_outgoing<14){
      saved_outgoing_points<-2
    }else if(average_saved_outgoing>4 && average_saved_outgoing<7){
      saved_outgoing_points<-1
    }else{
      saved_outgoing_points<-0
    }
    
    
  },error=function(e){})
  
  Avg_Call_dia_saved_cont<-saved_outgoing_points
  
  
  # saved_outgoing_count_calls<-data.frame(tryCatch({saved_outgoing_count_calls}, error=function(e){}))
  # saved_outgoing_points<-0
  # for(i in 1:nrow(saved_outgoing_count_calls)){
  #   tryCatch({
  #   if(saved_outgoing_count_calls[i,1]> 15)
  #   {
  #     saved_outgoing_points[i]<-3
  #   }
  #   else if(saved_outgoing_count_calls[i,1]>8 && saved_outgoing_count_calls[i,1]<14 ){
  #     
  #     saved_outgoing_points[i]<-2
  #   }
  #   else  if(saved_outgoing_count_calls[i,1]>4 && saved_outgoing_count_calls[i,1]<8){
  #     
  #     saved_outgoing_points[i]<-1
  #   }
  #   
  #   else {
  #     
  #     saved_outgoing_points[i]<-0
  #   }
  #   }, error=function(e){})
  # }
  # 
  # saved_outgoing_calls_points_result<-tryCatch({
  # saved_outgoing_calls_points_result<-cbind (saved_outgoing_count_calls,saved_outgoing_points)
  # 
  # }, error=function(e){})
  
  ###########################################################################################################
  
  ###  calls  done  to  unsaved contacts per day (> 15  :-- 0 , 8-14 :-- 1 ,4-8 :--2  ,else 3).
  
  average_unsaved_outgoing
  unsaved_outgoing_points<-0
  
  tryCatch({
    
    if(average_unsaved_outgoing>=15){
      unsaved_outgoing_points<-0
    } else if(average_unsaved_outgoing>8 && average_unsaved_outgoing<14){
      unsaved_outgoing_points<-1
    }else if(average_unsaved_outgoing>4 && average_unsaved_outgoing<7){
      unsaved_outgoing_points<-2
    }else{
      unsaved_outgoing_points<-3
    }
    
    
  },error=function(e){})
  
  Avg_Call_dia_unsaved_cont<-unsaved_outgoing_points
  
  
  # unsaved_outgoing_count_calls_all<-unsaved_outgoing_count_calls
  # 
  # unsaved_outgoing_count_calls<-data.frame(tryCatch({unsaved_outgoing_count_calls[2]}, error=function(e){}))
  # unsaved_outgoing_points<-0
  # for(i in 1:nrow(unsaved_outgoing_count_calls)){
  #   
  #   tryCatch({
  #   if(unsaved_outgoing_count_calls[i,1]> 15)
  #   {
  #     unsaved_outgoing_points[i]<-0
  #   }
  #   else if(unsaved_outgoing_count_calls[i,1]>8 && unsaved_outgoing_count_calls[i,1]<14 ){
  #     
  #     unsaved_outgoing_points[i]<-1
  #   }
  #   else  if(unsaved_outgoing_count_calls[i,1]>4 && unsaved_outgoing_count_calls[i,1]<8){
  #     
  #     unsaved_outgoing_points[i]<-2
  #   }
  #   
  #   else {
  #     unsaved_outgoing_points[i]<-3
  #     
  #   }
  #   }, error=function(e){})
  # }
  # 
  # unsaved_outgoing_calls_points_result<-tryCatch({
  # unsaved_outgoing_calls_points_result<-cbind (unsaved_outgoing_count_calls_all,unsaved_outgoing_points)
  # },error=function(e){})
  # #########################################################################################################
  
  ###  missed  calls  from  saved number per day (> 15  :-- 0 , 8-14 :-- 1 ,4-8 :--2  ,else 3)..
  
  
  average_saved_missedcall
  saved_missed_points<-0
  
  tryCatch({
    
    if(average_saved_missed>=15){
      saved_missed_points<-3
    } else if(average_saved_missed>8 && average_saved_missed<14){
      saved_missed_points<-2
    }else if(average_saved_missed>4 && average_saved_missed<7){
      saved_missed_points<-1
    }else{
      saved_missed_points<-0
    }
    
    
  },error=function(e){})
  
  Avg_Call_miss_saved_cont<-saved_missed_points
  # saved_missed_count_calls<-saved_missed_count_calls
  # saved_missed_count_calls<-data.frame(tryCatch({saved_missed_count_calls},error=function(e){}))
  # saved_missed_points<-0
  # for(i in 1:nrow(saved_missed_count_calls)){
  #   tryCatch({
  #   if(saved_missed_count_calls[i,1]> 15)
  #   {
  #     saved_missed_points[i]<-0
  #   }
  #   else if(saved_missed_count_calls[i,1]>8 && saved_missed_count_calls[i,1]<14 ){
  #     
  #     saved_missed_points[i]<-1
  #   }
  #   else  if(saved_missed_count_calls[i,1]>4 && saved_missed_count_calls[i,1]<8){
  #     
  #     saved_missed_points[i]<-2
  #   }
  #   
  #   else {
  #     saved_missed_points[i]<-3
  #     
  #   }
  #   },error=function(e){})
  # }
  # 
  # saved_missed_calls_points_result<-tryCatch({
  # saved_missed_calls_points_result<-cbind (saved_missed_count_calls,saved_missed_points)
  # 
  # },error=function(e){})
  ##################################################################################################
  ###  missed  calls  from  unsaved number per day (  >15 :-- 3  ,8-14 :--2    ,4- 8 :--1 , else  0) .
  average_unsaved_missedcall
  unsaved_missed_points<-0
  
  
  tryCatch({
    
    if(average_unsaved_missed>=15){
      unsaved_missed_points<-0
    } else if(average_unsaved_missed>8 && average_unsaved_missed<14){
      unsaved_missed_points<-1
    }else if(average_unsaved_missed>4 && average_unsaved_missed<7){
      unsaved_missed_points<-2
    }else{
      unsaved_missed_points<-3
    }
    
    
  },error=function(e){})
  
  Avg_Call_miss_unsaved_cont<-unsaved_outgoing_points
  
  # unsaved_missed_count_calls_all<-unsaved_missed_count_calls
  # unsaved_missed_count_calls<-data.frame(tryCatch({unsaved_missed_count_calls[2]},error=function(e){}))
  # unsaved_missed_points<-0
  # for(i in 1:nrow(unsaved_missed_count_calls)){
  #   tryCatch({
  #   if(unsaved_missed_count_calls[i,1]> 15)
  #   {
  #     unsaved_missed_points[i]<-3
  #   }
  #   else if(unsaved_missed_count_calls[i,1]>8 && unsaved_missed_count_calls[i,1]<14 ){
  #     
  #     unsaved_missed_points[i]<-2
  #   }
  #   else  if(unsaved_missed_count_calls[i,1]>4 && unsaved_missed_count_calls[i,1]<8){
  #     
  #     unsaved_missed_points[i]<-1
  #   }
  #   
  #   else {
  #     
  #     unsaved_missed_points[i]<-0
  #   }
  #    
  #   },error=function(e){})
  # }
  # 
  # 
  # unsaved_missed_calls_points_result<-tryCatch({
  # unsaved_missed_calls_points_result<-cbind (unsaved_missed_count_calls_all,unsaved_missed_points)
  # },error=function(e){})
  ###########################################################################################################################################################
  
  ### Saving all datas in a dataframe. 
  save_unsave_missed<<-0
  save_unsave_missed<<-data.frame(paste("user",pr,sep="_"),Avg_Call_rev_saved_cont,Avg_Call_rev_unsaved_cont,Avg_Call_dia_saved_cont,Avg_Call_dia_unsaved_cont,Avg_Call_miss_saved_cont,Avg_Call_miss_unsaved_cont) 
  
  }, error=function(e){
    
    save_unsave_missed<<-data.frame(paste("user",pr,sep="_"),Avg_Call_rev_saved_cont=0,Avg_Call_rev_unsaved_cont=0,Avg_Call_dia_saved_cont=0,Avg_Call_dia_unsaved_cont=0,Avg_Call_miss_saved_cont=0,Avg_Call_miss_unsaved_cont=0) 
    
    
    # user<<-data.frame(paste("user",tr,sep="_"),0,0 ,0,0, 0,0,0,0) 
    
    
  })
  
  
  return(save_unsave_missed)
}

y_saved_unsaved_missing<-data.frame()
y_saved_unsaved_missing<-do.call(rbind.data.frame,lapply(seq(call_S3_character),function(k){module_two(k) }))
#y_saved_unsaved_missing<-do.call(rbind.data.frame,lapply(seq(call_S3_character),function(k){tryCatch({module_two(k) }, error=function(e){})}))
colnames(y_saved_unsaved_missing)[1]<-"users"

########################################################################################

## 3. Duration analysis.

module_three<-function(prt){
  
  tryCatch({
  df<-aws_call(call_S3_character[prt])
  #df<-users1_calls
  df$date<-if( c("date")%in%names(df)=="FALSE")
    
  {
    df$date<-0
    
  }else{
    df$date<-df$date
  }
  
  df$normalized_number<-if( c("normalized_number")%in%names(df)=="FALSE")
    
  {
    df$normalized_number<-0
    
  }else{
    df$normalized_number<-df$normalized_number
  }
  
  df$type<-if( c("type")%in%names(df)=="FALSE")
    
  {
    df$type<-0
    
  }else{
    df$type<-df$type
  }
  
  
  #df$name<-ifelse((c("name")%in%names(df)=="FALSE"),0,df$name)
  df$name<-if( c("name")%in%names(df)=="FALSE")
    
  {
    df$name<-0
    
  }else{
    df$name<-df$name
  }
  
  
  
  #df$name<-ifelse((c("df$suggest_text_1")%in%names(df)=="FALSE"),0,df$df$suggest_text_1)
  df_date_parsed<-tbl_df(df) 
  ######################Changing  date(epoc format ) to  date & time format . 
  df_date_time<-as.POSIXct((df$date)/1000,origin="1970-01-01")
  df_date<-substr(df_date_time,1,11)
  df_time<-substr(df_date_time,11,20)
  df_date_parsed<-cbind(df,df_date,df_time)
  
  ########## taking out laast 10  digits of normalized number column .
  df_date_parsed$normalized_number<-as.character(df_date_parsed$normalized_number)
  df_date_parsed$normalized_number<-substr(df_date_parsed$normalized_number,nchar(df_date_parsed$normalized_number)-9,nchar(df_date_parsed$normalized_number))
  
  
  #1. Calculating average duration of calls perday for saved number .
  
  ###incoming,outgoing,missed .
  res_unique_day<-by(as.numeric(df_date_parsed$normalized_number),df_date_parsed$df_date,unique)  ##Unique number calls he got or called .
  res_unique_day_count<-data.frame(sapply(res_unique_day,length))  ## Number of unique number calls he got or called  .
  
  #####saved contacts details calls done or recieved.
  unique_duration_data<-df_date_parsed %>% select(df_date,name,duration)
  
  ######SAVED  NUMBERS DURATION ANALYSIS#################
  
  
  saved_unique_duration_data <-na.omit(unique_duration_data)
  saved_unique_duration_day_data_count<-by(saved_unique_duration_data$duration,saved_unique_duration_data$df_date,unique)
  saved_unique_count_day_data_count<-by(saved_unique_duration_data$name,saved_unique_duration_data$df_date,unique)
  saved_count_calls<-data.frame(sapply(saved_unique_count_day_data_count,length))
  saved_sum_calls<-data.frame(sapply(saved_unique_duration_day_data_count,sum))
  sum_count<-cbind(saved_sum_calls,saved_count_calls)
  average_incoming_saved_calls<-na.omit(round(sum_count[1]/sum_count[2]) ) ##Duration / Number of calls.)
  average_of_incoming_saved_calls<- sum(average_incoming_saved_calls)/nrow(average_incoming_saved_calls)
  
  
  ######UNSAVED  NUMBERS DURATION ANALYSIS#################
  
  unsaved_unique_duration_data <-subset(unique_duration_data, is.na(unique_duration_data$name))
  unsaved_unique_duration_day_data_count<-by(unsaved_unique_duration_data$duration,unsaved_unique_duration_data$df_date,unique)
  unsaved_unique_count_day_data_count<-by(unsaved_unique_duration_data$name,unsaved_unique_duration_data$df_date,count)
  unsaved_count_calls<-data.frame(sapply(unsaved_unique_duration_day_data_count,length))
  unsaved_sum_calls<-data.frame(sapply(unsaved_unique_duration_day_data_count,sum))
  sum_count<-data.frame(unsaved_sum_calls,unsaved_count_calls)
  average_unsaved_calls<-na.omit(round(sum_count[1]/sum_count[2]) ) ##Duration / Number of calls.
  
  #tryCatch({
  average_of_unsaved_calls<<-ifelse(nrow(average_unsaved_calls)==0,0,round(sum(average_unsaved_calls)/nrow(average_unsaved_calls)) )
  #},error=function(e){})
  
  ###  ******** Number of unsaved number need to  be verified *****
  
  ########CALL  DURATION BY TYPE ANALYSIS .
  
  unique_duration_data_call<-df_date_parsed %>% select(df_date,type,name,duration)
  
  
  ###### Call  duration  for unsaved number having incoming calls .
  
  number_of_incoming_duration <-unique_duration_data_call%>% subset(type==1) ##  subseting incoming calls .
  if(nrow(number_of_incoming_duration )>0){
    unsaved_number_incoming<-number_of_incoming_duration[is.na(number_of_incoming_duration$name),] ##  unsaved number incoming calls .
    #unsaved_number_incoming<-ifelse(nrow(unsaved_number_incoming)==0,0,unsaved_number_incoming)
    unsaved_number_incoming_duration<-by(unsaved_number_incoming$duration,unsaved_number_incoming$df_date,unique) #Unsaved number sum of duration incoming .
    unsaved_sum_incoming_sum_duration<-data.frame(sapply(unsaved_number_incoming_duration,sum))
    
    unsaved_number_incoming$name<-1 
    unsaved_number_incomin_calls<-by(unsaved_number_incoming$name,unsaved_number_incoming$df_date,sum)
    unsaved_number_incoming_calls_frequency<-t(data.frame(do.call("rbind", list(unsaved_number_incomin_calls))))
    
    
    
    average_duration_calls_incoming<-round(unsaved_sum_incoming_sum_duration/unsaved_number_incoming_calls_frequency)
    #average_call_duration_incoming_unsaved<-cbind(unsaved_sum_incoming_sum_duration,unsaved_number_incoming_calls_frequency,average_duration_calls_incoming)
    
    average_call_duration_incoming_unsaved<-round(sum(na.omit(average_duration_calls_incoming))/nrow(na.omit(average_duration_calls_incoming)))
    
    average_call_duration_for_incoming_unsaved<-ifelse(nrow(number_of_incoming_duration )==0,0,average_call_duration_incoming_unsaved)
    
  }else{
    average_call_duration_for_incoming_unsaved<-0
  }
  ###### Call  duration  for unsaved number having outgoing calls .
  
  
  number_of_outgoing_duration <-unique_duration_data_call%>% subset(type==2) ##  subseting outgoing calls .
  
  
  unsaved_number_outgoing<-number_of_outgoing_duration[is.na(number_of_outgoing_duration$name),] ##  unsaved number outgoing calls .
  if(nrow(unsaved_number_outgoing )>0){
    
    unsaved_number_outgoing_duration<-by(unsaved_number_outgoing$duration,unsaved_number_outgoing$df_date,unique) #Unsaved number sum of duration outgoing .
    unsaved_sum_outgoing_sum_duration<-data.frame(sapply(unsaved_number_outgoing_duration,sum))
    
    unsaved_number_outgoing$name<-1 
    unsaved_number_incomin_calls<-by(unsaved_number_outgoing$name,unsaved_number_outgoing$df_date,sum)
    unsaved_number_outgoing_calls_frequency<-t(data.frame(do.call("rbind", list(unsaved_number_incomin_calls))))
    
    average_duration_calls_outgoing<-round(unsaved_sum_outgoing_sum_duration/unsaved_number_outgoing_calls_frequency)
    average_call_duration_outgoing_unsaved<-cbind(unsaved_sum_outgoing_sum_duration,unsaved_number_outgoing_calls_frequency,average_duration_calls_outgoing)
    
    
    average_call_duration_for_outgoing_unsaved<-round(sum(na.omit(average_duration_calls_outgoing))/nrow(na.omit(average_duration_calls_outgoing)))
    
    #average_call_duration_for_outgoing_unsaved<-ifelse(nrow(number_of_outgoing_duration )==0,0,average_call_duration_for_incoming_unsaved)
    
  }else{
    
    average_call_duration_for_outgoing_unsaved<-0
  }
  
  ###### Call  duration  for unsaved number having missed calls .
  
  number_of_missed_duration <-unique_duration_data_call%>% subset(type==3) ##  subseting missed calls .
  
  unsaved_number_missed<-subset(number_of_missed_duration, is.na(number_of_missed_duration$name)) ##  unsaved number missed calls .
  if(nrow(unsaved_number_missed )>0){
    #unsaved_number_missed<-number_of_missed_duration[is.na(number_of_missed_duration),]
    unsaved_number_missed_duration<-by(unsaved_number_missed$duration,unsaved_number_missed$df_date,unique) #Unsaved number sum of duration missed .
    unsaved_sum_missed_sum_duration<-data.frame(sapply(unsaved_number_missed_duration,sum))
    
    unsaved_number_missed$name<-1
    unsaved_number_incomin_calls<-by(unsaved_number_missed$name,unsaved_number_missed$df_date,sum)
    unsaved_number_missed_calls_frequency<-t(data.frame(do.call("rbind", list(unsaved_number_incomin_calls))))
    
    average_duration_calls_missed<-round(unsaved_sum_missed_sum_duration/unsaved_number_missed_calls_frequency)
    average_call_duration_missed_unsaved<-cbind(unsaved_sum_missed_sum_duration,unsaved_number_missed_calls_frequency,average_duration_calls_missed)
    average_call_duration_for_missed_unsaved<-round(sum(na.omit(average_duration_calls_missed))/nrow(na.omit(average_duration_calls_missed)))
    
    #average_call_duration_for_missed_unsaved<-ifelse(nrow(number_of_missed_duration )==0,0,average_call_duration_for_incoming_unsaved)
    
    average_call_duration_for_missed_unsaved<-ifelse(nrow(number_of_missed_duration )==0,0,average_call_duration_for_incoming_unsaved)
    
  } else{
    average_call_duration_for_missed_unsaved<-0
  }
  ###### Call  duration  for saved number having incoming calls .
  
  unique_duration_data_call_saved<-na.omit(unique_duration_data_call)
  number_of_incoming_duration_saved <-unique_duration_data_call_saved%>% subset(type==1) ##  subseting incoming calls .
  
  if(nrow(number_of_incoming_duration_saved )>0){
    saved_number_incoming_duration<-by(number_of_incoming_duration_saved$duration,number_of_incoming_duration_saved$df_date,unique) #saved number sum of duration incoming .
    saved_sum_incoming_sum_duration<-data.frame(sapply(saved_number_incoming_duration,sum))
    
    
    saved_number_incomin_calls<-by(number_of_incoming_duration_saved$name,number_of_incoming_duration_saved$df_date,unique)
    saved_number_incoming_calls_frequency<-data.frame(sapply(saved_number_incomin_calls,length))
    
    average_duration_calls_incoming_saved<-round(saved_sum_incoming_sum_duration/saved_number_incoming_calls_frequency)
    average_call_duration_incoming_saved_result<-cbind(saved_sum_incoming_sum_duration,saved_number_incoming_calls_frequency,average_duration_calls_incoming_saved)
    #colnames(average_call_duration_for_incoming_saved_result)<-c("duration","number_of_calls","average_calls_per_day")
    average_call_duration_for_incoming_saved_result<-round(sum(na.omit(average_duration_calls_incoming_saved))/nrow(na.omit(average_duration_calls_incoming_saved)))
    
    average_call_duration_for_incoming_saved_result<-ifelse(nrow(na.omit(average_duration_calls_incoming_saved))==0,0,average_call_duration_for_incoming_saved_result)
  }else{
    average_call_duration_for_incoming_saved_result<-0
  }
  ###### Call  duration  for saved number having outgoing calls .
  
  unique_duration_data_call_saved<-na.omit(unique_duration_data_call_saved)
  number_of_outgoing_duration_saved <-unique_duration_data_call_saved%>% subset(type==2) ##  subseting outgoing calls .
  
  if(nrow(number_of_outgoing_duration_saved)>0){
    saved_number_outgoing_duration<-by(number_of_outgoing_duration_saved$duration,number_of_outgoing_duration_saved$df_date,unique) #saved number sum of duration outgoing .
    saved_sum_outgoing_sum_duration<-data.frame(sapply(saved_number_outgoing_duration,sum))
    
    
    saved_number_incomin_calls<-by(number_of_outgoing_duration_saved$name,number_of_outgoing_duration_saved$df_date,unique)
    saved_number_outgoing_calls_frequency<-data.frame(sapply(saved_number_incomin_calls,length))
    
    average_duration_calls_outgoing_saved<-round(saved_sum_outgoing_sum_duration/saved_number_outgoing_calls_frequency)
    average_call_duration_outgoing_saved_result<-cbind(saved_sum_outgoing_sum_duration,saved_number_outgoing_calls_frequency,average_duration_calls_outgoing_saved)
    #colnames(average_call_duration_outgoing_saved_result)<-c("duration","number_of_calls","average_calls_per_day")
    average_duration_calls_for_outgoing_saved<-round(sum(na.omit(average_duration_calls_outgoing_saved))/nrow(na.omit(average_duration_calls_outgoing_saved)))
    
    average_duration_calls_for_outgoing_saved<-ifelse(nrow(average_duration_calls_for_outgoing_saved)==0,0,average_duration_calls_for_outgoing_saved)
  }else{
    average_duration_calls_for_outgoing_saved<-0
  }
  ###############################################################################################################################
  ######################### Assigning points to the results #########################################
  
  ### point1:
  average_of_incoming_saved_calls
  average_of_incoming_saved_calls<-data.frame(tryCatch({average_of_incoming_saved_calls}, error=function(e){}))
  average_incoming_points<-0
  
  tryCatch({
    if(average_of_incoming_saved_calls> 300)
    {
      average_incoming_points<-3
    }
    else if(average_of_incoming_saved_calls>200 && average_of_incoming_saved_calls<300){
      
      average_incoming_points<-2
    }
    else  if(average_of_incoming_saved_calls>100 && average_of_incoming_saved_calls<100){
      
      average_incoming_points<-1
    }
    
    else {
      average_incoming_points<-0
      
    }
    
  }, error=function(e){})
  
  
  
  
  # saved_average_points_result<-cbind (average_of_incoming_saved_calls,average_incoming_points)
  Avg_Call_dur_saved_cont<-average_incoming_points
  
  
  
  
  
  
  ###point2 :
  average_of_unsaved_calls
  average_of_unsaved_calls[is.na(average_of_unsaved_calls)]<-0
  average_of_unsaved_calls<-data.frame(tryCatch({average_of_unsaved_calls}, error=function(e){}))
  average_points_unsaved<-0
  
  tryCatch({
    if(average_of_unsaved_calls> 300)
    {
      average_points_unsaved<-3
    }
    else if(average_of_unsaved_calls>200 && average_of_unsaved_calls<300){
      
      average_points_unsaved<-2
    }
    else  if(average_of_unsaved_calls>100 && average_of_unsaved_calls<100){
      
      average_points_unsaved<-1
    }
    
    else {
      average_points_unsaved<-0
      
    }
    
  }, error=function(e){})
  
  
  
  
  # unsaved_average_points_result<-cbind (average_of_unsaved_calls,average_points_unsaved)
  
  Avg_Call_dur_unsaved_cont<-average_points_unsaved
  
  
  
  ###point3 :
  average_call_duration_for_incoming_unsaved
  average_call_duration_for_incoming_unsaved[is.na(average_call_duration_for_incoming_unsaved)]<-0
  
  average_incoming_points_unsaved<-0
  
  tryCatch({
    if(average_call_duration_for_incoming_unsaved> 300)
    {
      average_incoming_points_unsaved<-3
    }
    else if(average_call_duration_for_incoming_unsaved>200 && average_call_duration_for_incoming_unsaved<300){
      
      average_incoming_points_unsaved<-2
    }
    else  if(average_call_duration_for_incoming_unsaved>100 &&average_call_duration_for_incoming_unsaved<100){
      
      average_incoming_points_unsaved<-1
    }
    
    else {
      average_incoming_points_unsaved<-0
      
    }
    
  }, error=function(e){})
  
  
  # unsaved_average_points_result<-cbind (average_call_duration_for_incoming_unsaved,average_incoming_points_unsaved)
  Avg_Call_dur_unsaved_inc_cont <- average_incoming_points_unsaved
  
  
  
  
  
  ##point4 :
  average_call_duration_for_outgoing_unsaved
  average_call_duration_for_outgoing_unsaved[is.na(average_call_duration_for_outgoing_unsaved)]<-0
  average_outgoing_unsaved<-0
  
  tryCatch({
    if(average_call_duration_for_outgoing_unsaved > 300)
    {
      average_outgoing_unsaved<-3
    }
    else if(average_call_duration_for_outgoing_unsaved>200 && average_call_duration_for_outgoing_unsaved<300){
      
      average_outgoing_unsaved<-2
    }
    else  if(average_call_duration_for_outgoing_unsaved>100 &&average_call_duration_for_outgoing_unsaved<100){
      
      average_outgoing_unsaved<-1
    }
    
    else {
      average_outgoing_unsaved<-0
      
    }
    
  }, error=function(e){})
  
  
  
  
  #unsaved_average_points_result<-cbind (average_call_duration_for_outgoing_unsaved,average_outgoing_unsaved)
  Avg_Call_dur_unsaved_out_cont<- average_outgoing_unsaved
  
  
  #################################################################################################
  
  ##point5 :
  average_call_duration_for_missed_unsaved[is.na(average_call_duration_for_missed_unsaved)]<-0
  
  average_missed_unsaved<-0
  
  tryCatch({
    if(average_call_duration_for_missed_unsaved> 300)
    {
      average_missed_unsaved<-3
    }
    else if(average_call_duration_for_missed_unsaved>200 && average_call_duration_for_missed_unsaved<300){
      
      average_missed_unsaved<-2
    }
    else  if(average_call_duration_for_missed_unsaved>100 &&average_call_duration_for_missed_unsaved<100){
      
      average_missed_unsaved<-1
    }
    
    else {
      average_missed_unsaved<-0
      
    }
    
  }, error=function(e){})
  
  
  
  # unsaved_average_missed_points_result<-cbind (average_call_duration_for_missed_unsaved,average_missed_unsaved)
  Avg_Call_dur_unsaved_mis_cont<- average_missed_unsaved
  
  
  
  ###############################################################################################################
  
  ##point6 :
  average_call_duration_for_incoming_saved_result
  average_call_duration_for_incoming_saved_result[is.na(average_call_duration_for_incoming_saved_result)]<-0
  average_saved_incoming<-0
  
  tryCatch({
    if(average_call_duration_for_incoming_saved_result> 300)
    {
      average_saved_incoming<-3
    }
    else if(average_call_duration_for_incoming_saved_result>200 && average_call_duration_for_incoming_saved_result<300){
      
      average_saved_incoming<-2
    }
    else  if(average_call_duration_for_incoming_saved_result>100 &&average_call_duration_for_incoming_saved_result<100){
      
      average_saved_incoming<-1
    }
    
    else {
      average_saved_incoming<-0
      
    }
    
  }, error=function(e){})
  
  
  #saved_average_points_result<-cbind (average_call_duration_for_incoming_saved_result,average_saved_incoming)
  Avg_Call_dur_saved_inc_cont<- average_saved_incoming
  
  
  ########################################################################################################
  ## point7:
  average_duration_calls_for_outgoing_saved
  
  average_duration_calls_for_outgoing_saved[is.na( average_duration_calls_for_outgoing_saved)]<-0
  average_saved_outgoing_call<-0
  
  tryCatch({
    if( average_duration_calls_for_outgoing_saved> 300)
    {
      average_saved_outgoing_call<-3
    }
    else if( average_duration_calls_for_outgoing_saved>200 &&  average_duration_calls_for_outgoing_saved<300){
      
      average_saved_outgoing_call<-2
    }
    else  if( average_duration_calls_for_outgoing_saved>100 && average_duration_calls_for_outgoing_saved<100){
      
      average_saved_outgoing_call<-1
    }
    
    else {
      average_saved_outgoing_call<-0
      
    }
    
  }, error=function(e){})
  
  
  
  # unsaved_average_points_result<-cbind (average_duration_calls_for_outgoing_saved,average_saved_outgoing_call)
  Avg_Call_dur_saved_out_cont<-average_saved_outgoing_call
  
  
  
  ################saving dataframe
  #Avg_Call_dur_unsaved_inc_cont<-data.frame()
  save_unsaved_duration<<-0
  
  save_unsaved_duration<<-data.frame(paste("user",prt,sep="_"),Avg_Call_dur_saved_cont,Avg_Call_dur_unsaved_cont,Avg_Call_dur_unsaved_inc_cont,Avg_Call_dur_unsaved_out_cont,Avg_Call_dur_unsaved_mis_cont,Avg_Call_dur_saved_inc_cont,Avg_Call_dur_saved_out_cont )
  
}, error=function(e){
  
  save_unsaved_duration<<-data.frame(paste("user",prt,sep="_"),Avg_Call_dur_saved_cont=0,Avg_Call_dur_unsaved_cont=0,Avg_Call_dur_unsaved_inc_cont=0,Avg_Call_dur_unsaved_out_cont=0,Avg_Call_dur_unsaved_mis_cont=0,Avg_Call_dur_saved_inc_cont=0,Avg_Call_dur_saved_out_cont=0 )
  
  
})
  
  return( save_unsaved_duration)
  #####################################################################
}

y_duration<-data.frame()
y_duration<-do.call(rbind.data.frame,lapply(seq(call_S3_character),function(k){module_three(k) }))

colnames(y_duration)[1]<-"users"


########################################################################################

## 4.  Weekly  frequency .



module_four<-function(four){
  tryCatch({
  df<-aws_call(call_S3_character[four])
  #df<-users11_calls
  df$date<-if( c("date")%in%names(df)=="FALSE")
    
  {
    df$date<-0
    
  }else{
    df$date<-df$date
    
  }
  
  df$normalized_number<-if( c("normalized_number")%in%names(df)=="FALSE")
    
    
  {
    df$normalized_number<-0
    
  }else{
    df$normalized_number<-df$normalized_number
  }
  
  df$type<-if( c("type")%in%names(df)=="FALSE")
    
  {
    df$type<-0
    
  }else{
    
    df$type<-df$type
  }
  
  
  #df$name<-ifelse((c("name")%in%names(df)=="FALSE"),0,df$name)
  df$name<-if( c("name")%in%names(df)=="FALSE")
    
  {
    df$name<-0
    
    
    
  }else{
    
    
    df$name<-df$name
  }
  
  
  
  #df$name<-ifelse((c("df$suggest_text_1")%in%names(df)=="FALSE"),0,df$df$suggest_text_1)
  
  df_date_parsed<-tbl_df(df) 
  
  
  
  
  ######################Changing  date(epoc format ) to  date & time format . 
  
  df_date_time<-as.POSIXct((df$date)/1000,origin="1970-01-01")
  
  
  df_date<-substr(df_date_time,1,11)
  df_time<-substr(df_date_time,11,20)
  
  df_date_parsed<-cbind(df,df_date,df_time)
  
  
  ########## taking out laast 10  digits of normalized number column .
  df_date_parsed$normalized_number<-as.character(df_date_parsed$normalized_number)
  df_date_parsed$normalized_number<-substr(df_date_parsed$normalized_number,nchar(df_date_parsed$normalized_number)-9,nchar(df_date_parsed$normalized_number))
  
  df_date_parsed$week<-lubridate::week(ymd(df_date_parsed$df_date))
  df_date_parsed<-data.frame(df_date_parsed)
  
  ################## Saved number calls weekly frequency.
  
  calls_weeks_number<-df_date_parsed%>%select(df_date,name,week) ## getting weeks,number , saved number.
  saved_data<-na.omit(calls_weeks_number)
  
  count_calls_weeks<-by(saved_data$name,saved_data$week,summary) ### People called number of times in weeks.
  
  arrange_saved_number<-do.call("list",count_calls_weeks)
  melting_list_items<-melt(arrange_saved_number)
  
  average_frequency_weekwise<-list(by(melting_list_items[,1],melting_list_items[,2],mean) ) ### taking out average of every week.
  average_frequency<-data.frame(t(round(do.call("rbind",average_frequency_weekwise))))
  average_frequency_weekly_final<-round(sum(average_frequency)/nrow(average_frequency))
  
  
  ################## Devide data into weeks ###############
  
  incoming_calls<-df_date_parsed%>%subset(type==1)  ## subsetting incoming calls 
  
  calls_week_incoming<-incoming_calls%>%select(df_date,week,name) ## incoming weeks,date,number .
  
  calls_week_incoming_saved<-na.omit(calls_week_incoming)
  
  count_calls_weeks_incoming<-by(calls_week_incoming_saved$name,calls_week_incoming_saved$week,summary) ### People called number of times in weeks.
  arrange_saved_number_incoming<-do.call("list",count_calls_weeks_incoming)
  melting_list_items_incoming<-melt(arrange_saved_number_incoming)
  melt_items_incoming<-melting_list_items_incoming[,1:2]
  average_frequency_weekwise_incoming<-list(by(melt_items_incoming[,1],melt_items_incoming[,2],mean) ) ### taking out average of every week.
  average_frequency_weekly_incoming<-data.frame(t(round(do.call("rbind",average_frequency_weekwise_incoming))))
  average_frequency_weekly_incoming_final<-round(sum(average_frequency_weekly_incoming)/nrow(average_frequency_weekly_incoming))
  
  
  ################## Outgoing number calls frequency.
  
  outgoing_calls<-df_date_parsed%>%subset(type==2)  ## subsetting outgoing calls 
  calls_week_outgoing<-outgoing_calls%>%select(df_date,week,name) ## outgoing weeks,date,number .
  calls_week_outgoing_saved<-na.omit(calls_week_outgoing)
  
  
  count_calls_weeks_outgoing<-by(calls_week_outgoing_saved$name,calls_week_outgoing_saved$week,summary) ### People called number of times in weeks.
  arrange_saved_number_outgoing<-do.call("list",count_calls_weeks_outgoing)
  melting_list_items_outgoing<-melt(arrange_saved_number_outgoing)
  melt_items_outgoing<-melting_list_items_outgoing[,1:2]
  average_frequency_weekwise_outgoing<-list(by(melt_items_outgoing[,1],melt_items_outgoing[,2],mean) ) ### taking out average of every week.
  average_frequency_weekly_outgoing<-data.frame(t(round(do.call("rbind",average_frequency_weekwise_outgoing))))
  rownames(average_frequency_weekly_outgoing)<-NULL
  average_frequency_weekly_outgoing_final<-round(sum(average_frequency_weekly_outgoing)/nrow(average_frequency_weekly_outgoing))
  
  
  ###########################Duration subset .
  
  duration_weeks_number<-df_date_parsed%>%select(df_date,week,duration,name) ## getting weeks,number , saved number.
  
  ################## Saved number duration  weekly frequency.
  saved_data_duration<-na.omit(duration_weeks_number)  ### removing unsaved number .
  saved_data_duration<-saved_data_duration%>%select(df_date,week,duration)
  count_duration_weeks<-by(saved_data_duration$duration,saved_data_duration$week,mean) ### People called number of times in weeks.
  arrange_saved_number_duration<-as.list(count_duration_weeks)
  melting_list_items_duration<-melt(arrange_saved_number_duration)
  melting_list_items_duration$value<-round(melting_list_items_duration$value)
  
  
  ###### Duration and frequency .
  
  duration_and_frequency_calls<-cbind(melting_list_items_duration,average_frequency_weekly_outgoing)
  colnames(duration_and_frequency_calls)<-c("duration","weeks","call_frequency")
  
  
  ### subsetting :
  #1. If frquency is greater than 15 and average duration >300 & 180 & 90 .
  
  fifteen_threehundred<-duration_and_frequency_calls%>%subset(call_frequency>15 && duration>300)
  average_fifteen_threehundred<-ifelse(nrow(fifteen_threehundred)>0,round(sum(fifteen_oneeighty)/fifteen_oneeighty),0)
  
  
  
  fifteen_oneeighty <-duration_and_frequency_calls%>%subset(call_frequency>15 && duration>180)
  average_fifteen_oneeighty<-ifelse(nrow(fifteen_oneeighty)>0,round(sum(fifteen_threehundred)/nrow(fifteen_threehundred)),0)
  
  
  fifteen_ninety <-duration_and_frequency_calls%>%subset(call_frequency>15 && duration>90)
  average_fifteen_ninety<-ifelse(nrow(fifteen_ninety)>0,round(sum(fifteen_ninety)/nrow(fifteen_ninety)),0)
  
  
  
  #2. If frquency is greater than 8 and average duration >300 &180 & 90 .
  
  eight_oneeighty<-duration_and_frequency_calls%>%subset(call_frequency>8 && duration>180)
  average_eight_oneeighty<-ifelse(nrow(eight_oneeighty)>0,round(sum(eight_oneeighty)/nrow(eight_oneeighty)),0)
  
  
  
  eight_three_hundred<-duration_and_frequency_calls%>%subset(call_frequency>8 && duration>300)
  average_eight_three_hundred<-ifelse(nrow(eight_three_hundred)>0,round(sum(eight_three_hundred)/nrow(eight_three_hundred)),0)
  
  
  
  eight_ninety<-duration_and_frequency_calls%>%subset(call_frequency>8 && duration>90)
  average_eight_ninety<-ifelse(nrow(eight_ninety)>0,round(sum(eight_ninety)/nrow(eight_ninety)),0)
  
  
  #1. If frquency is greater than 4 and average duration >300  &180  & 90.
  
  four_threehundred<-duration_and_frequency_calls%>%subset(call_frequency>4 && duration>300)
  average_four_threehundred<-ifelse(nrow(four_threehundred)>0,round(sum(four_threehundred)/nrow(four_threehundred)),0)
  
  
  
  four_oneeighty<-duration_and_frequency_calls%>%subset(call_frequency>4 && duration>180)
  average_four_oneeighty<-ifelse(nrow(four_oneeighty)>0,round(sum(four_oneeighty)/nrow(four_oneeighty)),0)
  
  
  
  four_ninety<-duration_and_frequency_calls%>%subset(call_frequency>4 && duration>90)
  average_four_ninety<-ifelse(nrow(four_ninety)>0,round(sum(four_ninety)/nrow(four_ninety)),0)
  
  
  
  
  
  ###################### assigning points to datapoints .
  
  
  ##point1 : average_frequency_weekly
  
  #average_frequency_weekly<-data.frame(average_frequency_weekly)
  Freq_avg_call_saved_cont<-0
  
  
  tryCatch({
    if(average_frequency_weekly_final>=15){
      Freq_avg_call_saved_cont<-3
      
    } else if(average_frequency_weekly_final>=8 &&  average_frequency_weekly_final<15){
      Freq_avg_call_saved_cont<-2
    }else if(average_frequency_weekly_final>=4 &&  average_frequency_weekly_final<8){
      Freq_avg_call_saved_cont<-1
      
    }else{
      
      Freq_avg_call_saved_cont<-0
    }
  },error=function(e){})
  
  #Freq_avg_call_saved_cont<-ave_freq_no_saved_no
  
  
  #################################################################
  
  ##point2 : average_frequency_weekly_incoming.
  
  Freq_avg_call_saved_inc_cont<-0
  
  tryCatch({
    if(average_frequency_weekly_incoming_final>=15){
      Freq_avg_call_saved_inc_cont<-3
      
    } else if(average_frequency_weekly_incoming_final>=8 &&  average_frequency_weekly_incoming_final<15){
      Freq_avg_call_saved_inc_cont<-2
    }else if(average_frequency_weekly_incoming_final>=4 &&  average_frequency_weekly_incoming_final<8){
      Freq_avg_call_saved_inc_cont<-1
      
    }else{
      
      Freq_avg_call_saved_inc_cont<-0
    }
    
  },error=function(e){})
  #Freq_avg_call_saved_inc_cont<-ave_freq_no_saved_no_incoming
  
  ##point3 : average_frequency_weekly_outgoing .
  
  #average_frequency_weekly_outgoing_final
  
  Freq_avg_call_saved_out_cont<-0
  
  tryCatch({
    if(average_frequency_weekly_outgoing_final>=15){
      Freq_avg_call_saved_out_cont<-3
      
    } else if(average_frequency_weekly_outgoing_final>=8 &&  average_frequency_weekly_outgoing_final<15){
      Freq_avg_call_saved_out_cont<-2
    }else if(average_frequency_weekly_outgoing_final>=4 &&  average_frequency_weekly_outgoing_final<8){
      Freq_avg_call_saved_out_cont<-1
      
    }else{
      
      Freq_avg_call_saved_out_cont<-0
    }
    
  },error=function(e){})
  
  ##point4 : duration_and frequency_calls .
  
  #duration_and_frequency_calls
  
  #1. If frquency is greater than 15 and average duration >300 & 180 .
  
  #average_fifteen_threehundred
  
  #average_fifteen_oneeighty
  
  
  Freq_avg_call_saved_dur_contact15<-0
  tryCatch({
    if(average_fifteen_threehundred==T)
    {
      Freq_avg_call_saved_dur_contact15<-5
    } else if(average_fifteen_oneeighty==T){
      Freq_avg_call_saved_dur_contact15<-4
    }else if(average_fifteen_ninety==T){
      Freq_avg_call_saved_dur_contact15<-2
    }else{
      Freq_avg_call_saved_dur_contact15<-0
    }
    
  },error=function(e){})
  
  #2. If frquency is greater than 8 and average duration >300 &180 & 90 .
  
  
  #average_eight_oneeighty
  #average_eight_three_hundred
  #average_eight_ninety
  
  Freq_avg_call_saved_dur_contact8<-0
  tryCatch({
    
    if(average_eight_three_hundred==T)
    {
      Freq_avg_call_saved_dur_contact8<-5
    } else if(average_fifteen_oneeighty==T){
      Freq_avg_call_saved_dur_contact8<-4
    }else if(average_eight_ninety==T){
      Freq_avg_call_saved_dur_contact8<-2
    }else{
      Freq_avg_call_saved_dur_contact8<-0
    }
    
  },error=function(e){})
  #3. If frquency is greater than 4 and average duration >300  &180  & 90.
  
  Freq_avg_call_saved_dur_contact4 <-0
  
  tryCatch({
    if(average_four_threehundred==T)
    {
      Freq_avg_call_saved_dur_contact4 <-5
    } else if(average_fifteen_oneeighty==T){
      Freq_avg_call_saved_dur_contact4 <-4
    }else if(average_four_ninety==T){
      Freq_avg_call_saved_dur_contact4 <-2
    }else{
      Freq_avg_call_saved_dur_contact4 <-0
    }
    
    
  },error=function(e){})
  
  ########################################################################
  
  weekly<<-0
  weekly<<-data.frame(paste("user",four,sep="_"),Freq_avg_call_saved_cont,Freq_avg_call_saved_inc_cont,Freq_avg_call_saved_out_cont,Freq_avg_call_saved_dur_contact15
                     
                     , Freq_avg_call_saved_dur_contact8, Freq_avg_call_saved_dur_contact4)
  
  }, error=function(e){
    
    weekly<<-data.frame(paste("user",four,sep="_"),Freq_avg_call_saved_cont=0,Freq_avg_call_saved_inc_cont=0,Freq_avg_call_saved_out_cont=0,Freq_avg_call_saved_dur_contact15=0
                       
                       , Freq_avg_call_saved_dur_contact8=0, Freq_avg_call_saved_dur_contact4=0)
    
  })
  return(weekly)
  
  
}


y_weekly_frequency<-data.frame()
y_weekly_frequency<-do.call(rbind.data.frame,lapply(seq(call_S3_character),function(k){module_four(k) }))

colnames(y_weekly_frequency)[1]<-"users"


########################################################################################

## 5. Saved type saved calls .


module_five<-function(fml){
  
  tryCatch({
  options(warn=-1)
  #df<-users1_calls
  
  df<-aws_call(call_S3_character[fml])
  
  df$date<-if( c("date")%in%names(df)=="FALSE")
    
  {
    df$date<-0
    
  }else{
    df$date<-df$date
  }
  
  df$normalized_number<-if( c("normalized_number")%in%names(df)=="FALSE")
    
  {
    df$normalized_number<-0
    
  }else{
    df$normalized_number<-df$normalized_number
  }
  
  df$type<-if( c("type")%in%names(df)=="FALSE")
    
  {
    df$type<-0
    
  }else{
    df$type<-df$type
  }
  
  
  #df$name<-ifelse((c("name")%in%names(df)=="FALSE"),0,df$name)
  df$name<-if( c("name")%in%names(df)=="FALSE")
    
  {
    df$name<-0
    
  }else{
    df$name<-df$name
  }
  
  
  
  #df$name<-ifelse((c("df$suggest_text_1")%in%names(df)=="FALSE"),0,df$df$suggest_text_1)
  df_date_parsed<-tbl_df(df) 
  ######################Changing  date(epoc format ) to  date & time format . 
  df_date_time<-as.POSIXct((df$date)/1000,origin="1970-01-01")
  df_date<-substr(df_date_time,1,11)
  df_time<-substr(df_date_time,11,20)
  df_date_parsed<-cbind(df,df_date_time,df_date,df_time)
  
  ########## taking out laast 10  digits of normalized number column .
  df_date_parsed$normalized_number<-as.character(df_date_parsed$normalized_number)
  df_date_parsed$normalized_number<-substr(df_date_parsed$normalized_number,nchar(df_date_parsed$normalized_number)-9,nchar(df_date_parsed$normalized_number))
  
  
  df_date_parsed$week<-lubridate::week(ymd(df_date_parsed$df_date))
  df_date_parsed<-data.frame(df_date_parsed)
  
  #####################################################################################################################
  
  ###### calls by saved numbers/day by name ("amma", "appa" etc)
  df_saved_number<-df_date_parsed%>%select(week,duration,name)
  saved_data_duration<-na.omit(df_saved_number)  ### removing unsaved number .
  
  string_name_tobe_matched<-c("Maa","Mom","Mother","maa","baba","Mummy","Amma","papa","father","Father","pops","pop","Appa","sister","behen","nani","chelie","chelli","brother","cheli","tamudu","tambi","tangachi","Anna","Akka")
  
  data_after_string_matching<-subset(saved_data_duration,name%in%string_name_tobe_matched) ## subseting string matching .
  
  average_called_family_week<-data_after_string_matching%>%group_by(week,name)%>%summarise(duration_mean=mean(duration))
  
  #####################################################################################################
  ############Saved calls  from 6am-4pm , average duration of incoming calls .
  
  calls_incoming_six_twlve<-df_date_parsed%>%subset(type==1)
  saved_calls_incoming_six_twlve<-na.omit(calls_incoming_six_twlve%>%select(df_date_time,duration,name))
  
  require( lubridate )
  
  calls_between_six_four_incoming<-na.omit(with(saved_calls_incoming_six_twlve ,saved_calls_incoming_six_twlve[ hour( saved_calls_incoming_six_twlve$df_date_time ) >= 6& hour( saved_calls_incoming_six_twlve$df_date_time ) < 16 , ] ))
  
  average_duration_calls_incoming_six_four<-round(calls_between_six_four_incoming%>%summarise(calls_between_six_four_incoming=mean(duration)))
  
  ################################################################################################
  
  
  ############Saved calls  from 6am-4pm , average duration of outgoing calls .
  
  calls_outgoing_six_twlve<-df_date_parsed%>%subset(type==2)
  saved_calls_outgoing_six_twlve<-na.omit(calls_outgoing_six_twlve%>%select(df_date_time,duration,name))
  
  require( lubridate )
  
  calls_between_six_four_outgoing<-with(saved_calls_outgoing_six_twlve ,saved_calls_outgoing_six_twlve[ hour( saved_calls_outgoing_six_twlve$df_date_time ) >= 2 & hour( saved_calls_outgoing_six_twlve$df_date_time ) < 16 , ] )
  
  average_duration_calls_outgoing_six_four<-round(calls_between_six_four_outgoing%>%summarise(calls_between_six_four_outgoing=mean(duration)))
  
  ################################################################################################
  
  ############Saved calls  from 5pm-12 am , average duration of incoming calls .
  
  calls_incoming<-df_date_parsed%>%subset(type==1)
  saved_calls_incoming<-na.omit(calls_incoming%>%select(df_date_time,duration,name))
  
  require( lubridate )
  tryCatch({
    calls_between_six_four_incoming<-na.omit(with(saved_calls_incoming, saved_calls_incoming[ hour( saved_calls_incoming$df_date_time ) >= 17& hour( saved_calls_incoming$df_date_time ) < 11:59 , ] )) },error=function(e){})
  
  average_duration_calls_incoming_five_twlve<-round(calls_between_six_four_incoming%>%summarise(average_duration_incoming=mean(duration)))
  
  ################################################################################################
  ############Saved calls  from 5pm-12 am , average duration of outgoing calls .
  
  calls_outgoing_five_twlve<-df_date_parsed%>%subset(type==2)
  saved_calls_outgoing_fivetwlve<-na.omit(calls_outgoing_five_twlve%>%select(df_date_time,duration,name))
  
  require( lubridate )
  
  calls_between_fice_twlve_outgoing<-na.omit(with(saved_calls_outgoing_fivetwlve,saved_calls_outgoing_fivetwlve[ hour( saved_calls_outgoing_fivetwlve$df_date_time ) >= 17& hour( saved_calls_outgoing_fivetwlve$df_date_time ) < 11:59 , ] )) 
  
  average_duration_calls_outgoing_five_twlve<-round(calls_between_fice_twlve_outgoing%>%summarise(calls_between_fice_twlve_outgoing=mean(duration)))
  
  ################################################################################################
  
  ################################################################################################
  ############Saved calls  from 12am-3am , average duration of incoming calls .
  
  calls_incoming_twlve_three<-df_date_parsed%>%subset(type==1)
  saved_calls_incoming_twlve_three<-na.omit(calls_incoming_twlve_three%>%select(df_date_time,duration,name))
  
  require( lubridate )
  
  calls_between_twlve_three_incoming<-na.omit(with(saved_calls_incoming_twlve_three, saved_calls_incoming_twlve_three[ hour( saved_calls_incoming_twlve_three$df_date_time ) >= 11:59 & hour( saved_calls_incoming_twlve_three$df_date_time ) < 3, ] ))
  
  average_duration_calls_incoming_twlve_three<-round(calls_between_twlve_three_incoming=mean(duration))
  
  
  ################################################################################################
  
  ################################################################################################
  ############Saved calls  from 12am-3am , average duration of outgoing calls .
  
  calls_outgoing_twlve_three<-df_date_parsed%>%subset(type==2)
  saved_calls_outgoing_twlve_three<-na.omit(calls_outgoing_twlve_three%>%select(df_date_time,duration,name))
  
  require( lubridate )
  
  
  calls_between_twlve_three_outgoing<-na.omit(with(saved_calls_outgoing_twlve_three, saved_calls_outgoing_twlve_three[ hour( saved_calls_outgoing_twlve_three$df_date_time ) >= 11:59 & hour( saved_calls_outgoing_twlve_three$df_date_time ) < 3, ] ))
  
  average_duration_calls_outgoing_twlve_three<-round(calls_between_twlve_three_outgoing=mean(duration))
  
  
  ################################################################################################
  
  ######## Getting  the URL's of holiday list .
  library(XML)
  library(xml2)
  library(rvest)
  URL <- "http://www.officeholidays.com/countries/india/2017.php"
  
  holiday_scraped_list<-readHTMLTable(URL, header=T, which=1,stringsAsFactors=F)
  holiday<-c("New Years Day","Bhogi","Pongal","Republic Day","Maha Shivratri","Holi","Ugadi","Telugu New Year","Odisha Day","Ram Navami",
             
             "Mahavir Jayanti","Good Friday","Tamil New Year","Bengali New Year","Maharashtra Day","Ratha Yatra","Idul Fitr","Raksha Bandhan",
             "Janmashtami","Independence Day","Ganesh Chaturthi","Idul Juha","Muharram","Dussehra",
             "Vijaya Dashami","Mahatma Gandhi Birthday","Deewali","Deepavali","Kannada Rajyothsava",
             "Christmas Day")
  
  
  holiday_list<-subset(holiday_scraped_list,Holiday%in%holiday)
  duplicate_holiday<-holiday_list[,3]
  duplicated_holiday_data<-duplicate_holiday[duplicated(duplicate_holiday)]
  final_duplicated_holidays<-subset(holiday_list,Holiday%in%duplicated_holiday_data)
  comments_on_holidays_subset<-c("Celebrated on the 13th night/14th day in the Krishna Paksha",
                                 "Telugu and Kannada New Year. Andhra Pradesh, Karnataka, Telangana",
                                 "Celebrates the birth of Lord Rama to King Dasharatha of Ayodhya",
                                 "Many states","Celebrates the birth of Lord Shri Krishna",
                                 "Many states","Vijaya Dashami","Deepawali")
  subset_duplicated_columns<-subset(final_duplicated_holidays,Comments%in%comments_on_holidays_subset)
  
  unique_holidays<-holiday_list[!(duplicated(holiday_list$Holiday) | duplicated(holiday_list$Holiday, fromLast = TRUE)), ]
  
  final_holiday_list<-rbind(unique_holidays,subset_duplicated_columns)
  
  ############ Analysing call duration and call  according to  holidays.
  
  data_for_holiday<-df_date_parsed%>%select(df_date,duration)
  
  data_for_holiday$day_format<-format(as.Date(data_for_holiday$df_date), "%0b %d")
  
  final_holiday_list_truncate<-gsub("^.*\\\n","", final_holiday_list$Date)   ##  truncated list of holidays.
  
  trim <- function (x) gsub("^\\s+|\\s+$", "", x) ###  function to remove whitespace.
  
  final_holiday_list_truncate<-trim(final_holiday_list_truncate)
  
  kll<-gsub("^.\\* ","", final_holiday_list_truncate)
  
  library(stringr)
  str_splt1<-str_split_fixed(final_holiday_list_truncate, " ",2)
  
  pss<-ifelse(nchar(str_splt1[,2])==1,paste(0,str_splt1[,2]),str_splt1[,2])
  ess<-gsub(" ", "", pss) 
  
  joined_string<-paste(str_splt1[,1],ess)
  
  subset_data_according_toholidays<-subset(data_for_holiday,day_format%in%joined_string)
  
  ################################################################################################
  
  ##########################Result sets .
  
  ##point 1 : Calls to saved type family per day.
  average_called_family_week<-data.frame(average_called_family_week)
  tryCatch({
    closed_one_calls<-0
    
    if(average_called_family_week>=700){
      closed_one_calls<-5
    }else if(average_called_family_week>450 &&  average_called_family_week<700){
      closed_one_calls<-3
    }else if(average_called_family_week>275 &&  average_called_family_week<= 450 ){
      closed_one_calls<-1
    }else{
      closed_one_calls<-0
    }
    
  },error=function(e){})
  
  Freq_avg_call_saved_cont<- closed_one_calls
  #final_calls_closed_ones_result<-cbind(data.frame(average_called_family_week),closed_one_calls)
  
  
  ###################################################################################################
  
  ##point2 : Saved calls by time from 6am to 4pm incoming .
  average_duration_calls_incoming_six_four
  
  tryCatch({
    average_incoming_sixfour<-0
    if(average_duration_calls_incoming_six_four>=250)
    {
      average_incoming_sixfour<-3
      
    }
    if(average_duration_calls_incoming_six_four>125 &&  average_duration_calls_incoming_six_four<250)
    {
      average_incoming_sixfour<-2
    }
    
    if(average_duration_calls_incoming_six_four>50 && average_duration_calls_incoming_six_four<75)
    {
      average_incoming_sixfour<-1
    }
    
  },error=function(e){})
  
  Time_based_saved_6am_4pm_avg_inc_calls_dur<-average_incoming_sixfour
  #average_income_sixfour_result<-cbind(average_duration_calls_incoming_six_four, average_incoming_sixfour)
  
  #####################################################################################################
  
  
  ###point3 : Saved calls by time from 6am to 4pm outgoing .
  average_duration_calls_outgoing_six_four
  
  tryCatch({
    average_outgoing_sixfour<-0
    if(average_duration_calls_outgoing_six_four>=250)
    {
      average_outgoing_sixfour<-3
      
    }
    if(average_duration_calls_outgoing_six_four>125 &&  average_duration_calls_outgoing_six_four<250)
    {
      average_outgoing_sixfour<-2
    }
    
    if(average_duration_calls_outgoing_six_four>50 && average_duration_calls_outgoing_six_four<75)
    {
      average_outgoing_sixfour<-1
    }
    
  },error=function(e){})
  
  Time_based_saved_6am_4pm_avg_out_calls_dur<-average_outgoing_sixfour
  #average_income_sixfour_result<-cbind(average_duration_calls_outgoing_six_four, average_outgoing_sixfour)
  
  #########################################################################################################
  
  ###point 4 :Saved calls by time from 5pm to 12am incoming
  
  average_duration_calls_incoming_five_twlve
  tryCatch({
    
    average_incoming_fivetwlve<-0
    if(average_duration_calls_incoming_five_twlve>=250)
    {
      average_incoming_fivetwlve<-3
      
    }
    if(average_duration_calls_incoming_five_twlve>125 &&  average_duration_calls_incoming_five_twlve<250)
    {
      average_incoming_fivetwlve<-2
    }
    
    if(average_duration_calls_incoming_five_twlve>50 && average_duration_calls_incoming_five_twlve<75)
    {
      average_incoming_fivetwlve<-1
    }
    
  },error=function(e){})
  
  Time_based_saved_5pm_12am_avg_inc_calls_dur<-average_incoming_fivetwlve
  #average_income_fivetwlve_result<-cbind(average_duration_calls_incoming_five_twlve, average_incoming_fivetwlve)
  
  ###############################################################################################################
  
  ###point 5 :Saved calls by time from 5pm to 12am outgoing.
  
  average_duration_calls_outgoing_five_twlve
  
  tryCatch({
    average_outgoing_fivetwlve<-0
    
    
    if(average_duration_calls_outgoing_five_twlve>=250)
    {
      average_outgoing_fivetwlve<-3
      
    }
    if(average_duration_calls_outgoing_five_twlve>125 &&  average_duration_calls_outgoing_five_twlve<250)
    {
      average_outgoing_fivetwlve<-2
    }
    
    if(average_duration_calls_outgoing_five_twlve>50 && average_duration_calls_outgoing_five_twlve<75)
    {
      average_outgoing_fivetwlve<-1
    }
    
  },error=function(e){})
  
  Time_based_saved_5pm_12am_avg_out_calls_dur <- average_outgoing_fivetwlve
  #average_outgoing_fivetwlve_result<-cbind(average_duration_calls_outgoing_five_twlve, average_outgoing_fivetwlve)
  
  ################################################################################################################
  
  ##points 6 : Saved calls from 12am to 3am incoming
  tryCatch({
    average_duration_calls_incoming_twlve_three
    
    average_incoming_twelvethree<-0
    if(average_duration_calls_incoming_twelve_three>=250)
    {
      average_incoming_twelvethree<-3
      
    }
    if(average_duration_calls_incoming_twelve_three>125 &&  average_duration_calls_incoming_twelve_three<250)
    {
      average_incoming_twelvethree<-2
    }
    
    if(average_duration_calls_incoming_twelve_three>50 && average_duration_calls_incoming_twelve_three<75)
    {
      average_incoming_twelvethree<-1
    }
  },error=function(e){})
  
  Time_based_saved_12am_3am_avg_inc_calls_dur <-average_incoming_twelvethree
  #average_income_twelvethree_result<-cbind(average_duration_calls_incoming_twelve_three, average_incoming_twelvethree)
  
  #################################################################################################
  
  ##points 6 : Saved calls from 12am to 3am outgoing .
  
  average_duration_calls_outgoing_twlve_three
  
  tryCatch({
    
    average_outgoing_twelvethree<-0
    
    
    if(average_duration_calls_outgoing_twelve_three>=250)
    {
      average_outgoing_twelvethree<-3
      
    }
    if(average_duration_calls_outgoing_twlve_three>125 &&  average_duration_calls_outgoing_twlve_three<250)
    {
      average_outgoing_twelvethree<-2
    }
    
    if(average_duration_calls_outgoing_twlve_three>50 && average_duration_calls_outgoing_twlve_three<75)
    {
      average_outgoing_twelvethree<-1
    }
  },error=function(e){})
  
  #average_outgoing_twelvethree_result<-cbind(average_duration_calls_outgoing_twlve_three, average_outgoing_twelvethree)
  
  Time_based_saved_12am_3am_avg_out_calls_dur<-average_outgoing_twelvethree
  #############################################################################################
  ##############assigning points 
  family1<<-0
  family1<<-data.frame(paste("user",fml,sep="_"),Freq_avg_call_saved_cont,Time_based_saved_6am_4pm_avg_inc_calls_dur,Time_based_saved_6am_4pm_avg_out_calls_dur,Time_based_saved_5pm_12am_avg_inc_calls_dur,
                     
                     Time_based_saved_5pm_12am_avg_out_calls_dur ,  Time_based_saved_12am_3am_avg_out_calls_dur    )

  
  }, error=function(e){
    
    family1<<-data.frame(paste("user",fml,sep="_"),Freq_avg_call_saved_cont=0,Time_based_saved_6am_4pm_avg_inc_calls_dur=0,Time_based_saved_6am_4pm_avg_out_calls_dur=0,Time_based_saved_5pm_12am_avg_inc_calls_dur=0,
                        
                        Time_based_saved_5pm_12am_avg_out_calls_dur=0 ,  Time_based_saved_12am_3am_avg_out_calls_dur =0   )
    
  })
    return(family1)
}

y_family_frequency<-data.frame()
y_family_frequency<-do.call(rbind.data.frame,lapply(seq(call_S3_character),function(k){module_five(k) }))

colnames(y_family_frequency)[1]<-"users"

############Joining multiple modules .

join_1<-left_join(y_call,y_duration,by="users")
join_2<-left_join(join_1,y_family_frequency,by="users")
join_3<-left_join(join_2,y_saved_unsaved_missing,by="users")
join_4<-left_join(join_3,y_weekly_frequency,by="users")
calls_scoring_results<-cbind(id,join_4)

########################################################################################

library(RMongo)
library(rmongodb)
 library(mongolite)
 
c = mongo(collection = "calls_scoring_results", db = "calls_data") ## Creating/using db and to make a connection .
 c$insert(calls_scoring_results) ## insert data  into the collection "y".  

  
 c$count()  
 
mongo = mongo.create(host = "localhost")
mongo.is.connected(mongo)

mongo.get.databases(mongo)   ## to  see  list  of databases .

mongo.get.database.collections(mongo, db = "ashis123")  ####  to  see  all  collections  inside a database .

DBNS = "calls_data.calls_scoring_results"
mongo.count(mongo, ns = DBNS)  ##finding number of rows in the collections/rows.


all_datas<-mongo.find.all(mongo, ns="calls_data.calls_scoring_results")  ## To   get all datas of a collection .

