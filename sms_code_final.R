############### Bank based transactions of the user  .


library(dplyr)
library(aws.s3)
library(RJSONIO)
library(plyr)
library(stringi)
library(stringr)

# ########################################
# ######################## Fetching  Data from S3 database.
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
# ##########################################################################################

####taking out  userids .

id<- gsub("sms/","",call_S3_character)



sms_data_analysis<-function(sms){

 tryCatch({
   df<-aws_call(call_S3_character[sms])
library(lubridate)

df_date_time<-as.POSIXct((df$date)/1000,origin="1970-01-01")
df_date<-substr(df_date_time,1,11)
df_time<-substr(df_date_time,11,20)
df_date_parsed<-cbind(df,df_date_time,df_date,df_time)

df<-df_date_parsed
trans_subset_all<-data.frame(df$body,df_date)  ##  all  messages .
colnames(trans_subset_all)<-c("body","date")

#############################libraries#########################
library(stringi)
library(stringr)
library(lubridate)
library(dplyr)
library(qdapRegex)
#############################################################

######################### from first module 


#########################################################################################
##### How many  sms recieved per day .
df_date_parsed$day<-day(df_date_parsed$df_date)
quantity_of_sms_per_day<-by(df_date_parsed$address,df_date_parsed$df_date,unique)
finding_number_of_counts_sms<-data.frame(sapply(quantity_of_sms_per_day,length))
row.names(finding_number_of_counts_sms)<-NULL
average_number_of_sms<- round(mean(finding_number_of_counts_sms[,1]))

# #######################################################################################


# Deviding data into contact based , promotional ,transactional on the basis of matching strings .

##########################################################
## Promotional based sms subsetting .

lowercase_smsdata<-tolower(df_date_parsed$body)
sorting_array<- tolower(c("Service provider","MB","121", "121","Recharge", "Unlimited", "offer", "3G"," 4G", "2G", "Dial", "28 days"," GB"," Calls", "Talktime", "Free"," TT", " in one word etc"," 0ff","%", "Cashback", "Fees", "account talktime", "Voucher", "Valid", "today", "Customer", "local+std", "tollfree", "network"))

put<-length(lowercase_smsdata)
get<-length(sorting_array)
output<-matrix(ncol=get,nrow=put)

rt<-0

tryCatch({
for(i in 1:put)
{
  
  output[,i]<- str_count(lowercase_smsdata,sorting_array[i])
  
}

  }, error=function(e){})
sum_thecount<-data.frame(rowSums(output))

matching_count_result<-cbind(df_date_parsed$address,df_date_parsed$body,df_date_parsed$df_date,df_date_parsed$day,sum_thecount) ## finding the count of the string matching .

promotional_msg_count_analysis<-matching_count_result%>%subset(matching_count_result[,5]>3)

###################################################################################################################

### Analysis baased on Address .

df_date_parsed_address<-data.frame(df_date_parsed$address,df_date_parsed$body,df_date_parsed$df_date,df_date_parsed$day) 

df_date_parsed_address[,1]<-as.character(df_date_parsed_address[,1])
count_t<-nchar(df_date_parsed_address[,1])
datafrme<-cbind(df_date_parsed_address,count_t)

less_than_10<-datafrme%>%subset(as.numeric(datafrme[,5])< 10)

greater_than_10<-datafrme%>%subset(as.numeric(datafrme[,5])>= 10)

#contact_based_sms<-datafrme[greater_than_10[,1],]

taking_out_character_values<-grep("[A-Z,a-z]",greater_than_10[,1])

sorted_character<-greater_than_10[taking_out_character_values,]


binding_characters_from_other_numeric_data<-rbind(less_than_10,sorted_character) ## promotional and transactional data .


contact_based_values<-greater_than_10[-taking_out_character_values,] 

all_contact_based_data<-contact_based_values

### tranactional messages taking out .
### applying sorting based on character matching from body .

lowercase_body_address<-tolower(binding_characters_from_other_numeric_data[,2])
sorting_array<- tolower(c("Service provider","MB","121", "121","Recharge", "Unlimited", "offer", "3G"," 4G", "2G", "Dial", "28 days"," GB"," Calls", "Talktime", "Free"," TT", " in one word etc"," 0ff","%", "Cashback", "Fees", "account talktime", "Voucher", "Valid", "today", "Customer", "local+std", "tollfree", "network","toll-free"))

put_address<-length(lowercase_body_address)
get_address<-length(sorting_array)
output_address<-matrix(ncol=get,nrow=put_address)

tryCatch({
for(i in 1:put_address)
{
  
  output_address[,i]<- str_count(lowercase_body_address,sorting_array[i])
  
}

}, error=function(e){})
sum_thecount_address<-data.frame(rowSums(output_address))

matching_count_address_result<-cbind(binding_characters_from_other_numeric_data[,1],binding_characters_from_other_numeric_data[,2],binding_characters_from_other_numeric_data[,3],binding_characters_from_other_numeric_data[,4],binding_characters_from_other_numeric_data[,5],sum_thecount_address) ## finding the count of the string matching .

promotional_msg_count_address_analysis<-matching_count_address_result%>%subset(matching_count_address_result[,6]>3)

left_trans_promotional_msg<-matching_count_address_result[-as.numeric(row.names(promotional_msg_count_address_analysis)),]

numeric_value_deletion<-grep("[0-9]",left_trans_promotional_msg[,1])

data_numeric_value_deletion<-left_trans_promotional_msg[numeric_value_deletion,]

left_trans_promotional_msg_numeric_removel<-left_trans_promotional_msg[-numeric_value_deletion,]

all_promotional_data<-rbind(promotional_msg_count_address_analysis,data_numeric_value_deletion)

########################################

matching_body_transactional<-"a/c|account|ac|xxxx|bal|debit|debited|Recharge|txn| Available|balance|Rs.|your account|towards|cash withdrawal|atm withdrawals|withdrawals|amount of INR|An amount|Avail.|Bal| bank|  a/c|  refunded|your account no| pos|pos withdrawal| recharge done|Transid| mrp| txn id|initiated|available balance|online recharge txn id|debited with|otp|verify mobile|your otp|Rs|sb| gone below| minimum balance|order|your order|successfully placed|will be delivered|EPF balance|Amt:Rs.|Amt|Account updated|bookingno|delivered shortly|top up recharge successful|ola! Your bill for| Distance:|Time:|your ola| recharge of rs.|order id|recharge successful| avl bal|ECS|NACH| towards ECS|avoid penalty| monthly avg|monthly average| min balance|minimum balance|previous not maintained| one time password|debit card|credit card|credit| bank debit| bank credit|pos transaction|top up|refund|wallet"
strng_match<-grepl(matching_body_transactional,left_trans_promotional_msg_numeric_removel[,2])

strng_match_transaction_final<-left_trans_promotional_msg_numeric_removel[strng_match,]

t<-which(strng_match=="FALSE")

rest_transactional_final_data<-left_trans_promotional_msg_numeric_removel[t,]

promotional_data_final<-rbind(all_promotional_data,rest_transactional_final_data)

#################################################################################################

###  important variables :

##1 .binding_characters_from_other_numeric_data : contains all promotional and transactional messages .

##2.promotional_data_final : Contains all promotional data. 

##3 . strng_match_transaction_final : all transactional data .

## 4. all_contact_based_data : all  contact based data.

####################Promotional data analysis .
#promotional_data_final 

average_sms_day_promotional<-promotional_data_final[,2:3]

unique_sms_per_day<-by(average_sms_day_promotional[,1],average_sms_day_promotional[,2],unique)
count_the_number_ofsms_perday<-data.frame(sapply(unique_sms_per_day,length))

average_promotional_sms<-round(sum(count_the_number_ofsms_perday[,1])/nrow(count_the_number_ofsms_perday))

#############################################################################################
#### condition 1 : If promotional based average sms per day is > 15 award 5 else if > 10 award 3 else if > 4 award 1 else 0

point_promotioanal<-0
if(average_promotional_sms>15)
{
  point_promotioanal<-5
}else if(average_promotional_sms>10){
  point_promotioanal<-3
}else if(average_promotional_sms>4){
  point_promotioanal<-1
}else{
  point_promotioanal<-0
}

Frequency_promotional_based_sms_per_day<- point_promotioanal
### condition 2:  If promotional based average sms weekdays is > 30 award 5 else if > 15 award 3 else if > 8 award 1 else 0.

###  finding out  sms during weekdays .
##average_sms_day_promotional

library(lubridate)
date_subset<-weekdays(as.Date(average_sms_day_promotional[,2]))
dayname_converted<-cbind(average_sms_day_promotional,date_subset)

#weekend_subset <-dayname_converted%>%subset(date_subset %in% c("Saturday","Sunday"))
weekday_subset<-dayname_converted%>%subset(date_subset%in%c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))

unique_sms_per_day_weekdays<-by(weekday_subset[,1],weekday_subset[,2],unique)
count_the_number_ofsms_perday_weekdays<-data.frame(sapply(unique_sms_per_day_weekdays,length))

average_promotional_sms_weekdays<-round(sum(count_the_number_ofsms_perday_weekdays[,1])/nrow(count_the_number_ofsms_perday_weekdays))


point_promotioanal_weekday<-0
if(average_promotional_sms_weekdays>30){
  point_promotioanal_weekday<-5
}else if(average_promotional_sms_weekdays>15){
  point_promotioanal_weekday<-3
}else if(average_promotional_sms_weekdays>8){
  point_promotioanal_weekday<-1
}else{
  point_promotioanal_weekday<-0
}

Frequency_promotional_based_sms_weekly<-point_promotioanal_weekday

##########################################################################################################
##Contact based analysis .
#all_contact_based_data 

## cond1: if contact based average sms per day is > 10 award 5 else if > 7 award 3 else if > 3 award 1 else 0.

average_sms_day_contact_based<-all_contact_based_data [,2:3]

unique_sms_per_day_contact_based<-by(average_sms_day_contact_based[,1],average_sms_day_contact_based[,2],unique)

count_the_number_ofsms_perday_contactbased<-data.frame(sapply(unique_sms_per_day_contact_based,length))

average_contactbased_sms<-round(sum(count_the_number_ofsms_perday_contactbased[,1])/nrow(count_the_number_ofsms_perday_contactbased))

contact_based_total<-0

if(average_contactbased_sms>10){
  contact_based_total<-5
}else if(average_contactbased_sms>=7){
  contact_based_total<-3
}else if(average_contactbased_sms>3 &&average_contactbased_sms<7){
  contact_based_total<-1
}else{
  contact_based_total<-0
}

Frequency_contact_based_count_perday<- contact_based_total
## condition 2:  if contact based average sms weekdays is > 15 award 5 else if > 10 award 3 else if > 5 award 1 else 0.

#average_sms_day_contact_based
library(lubridate)
date_subset_contact<-weekdays(as.Date(average_sms_day_contact_based[,2]))
dayname_converted_contact<-cbind(average_sms_day_contact_based,date_subset_contact)

weekday_subset_contact_baased<-dayname_converted_contact%>%subset(date_subset_contact%in%c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))

unique_sms_per_day_weekdays_contacts<-by(weekday_subset_contact_baased[,1],weekday_subset_contact_baased[,2],unique)
count_the_number_of_sms_perday_weekdays_contacts<-data.frame(sapply(unique_sms_per_day_weekdays_contacts,length))

average_contact_sms_weekdays<-round(sum(count_the_number_of_sms_perday_weekdays_contacts[,1])/nrow(count_the_number_of_sms_perday_weekdays_contacts))

contact_weekdays_count<-0

if(average_contact_sms_weekdays>15){
  contact_weekdays_count<-5
}else if(average_contact_sms_weekdays>=10){
  contact_weekdays_count<-3
}else if(average_contact_sms_weekdays>5 && average_contact_sms_weekdays<10){
  contact_weekdays_count<-1
}else{
  contact_weekdays_count<-0
}

Frequency_contact_based_count_weekly<-  contact_weekdays_count

########################################################################################################
### Transactional based analysis .

#strng_match_transaction_final

##cond1:  If transactional bases average sms per day is > 1 award 5 else if > 0.5 award 3 else if > 0.2 award 1 else 0.
average_sms_day_transactional_based<-strng_match_transaction_final [,2:3]

unique_sms_per_day_transactional_based<-by(average_sms_day_transactional_based[,1],average_sms_day_transactional_based[,2],unique)

count_the_number_ofsms_perday_transactionalbased<-data.frame(sapply(unique_sms_per_day_transactional_based,length))

average_transactionalbased_sms<-round(sum(count_the_number_ofsms_perday_transactionalbased[,1])/nrow(count_the_number_ofsms_perday_transactionalbased))

transactional_based_total<-0

if(average_transactionalbased_sms>10){
  transactional_based_total<-5
}else if(average_transactionalbased_sms>=7){
  transactional_based_total<-3
}else if(average_transactionalbased_sms>=3 &&average_transactionalbased_sms<7){
  transactional_based_total<-1
}else{
  transactional_based_total<-0
}

Frequency_count_Transactional_based_daily<-  transactional_based_total

### condition2 :If transactional bases average sms weekdays is > 10 award 5 else if > 5 award 3 else if > 2 award 1 else 0.

#strng_match_transaction_final
library(lubridate)
date_subset_transactional<-weekdays(as.Date(average_sms_day_transactional_based[,2]))
dayname_converted_transactional<-cbind(average_sms_day_transactional_based,date_subset_transactional)

weekday_subset_transactional_baased<-dayname_converted_transactional%>%subset(date_subset_transactional%in%c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))

unique_sms_per_day_weekdays_transactionals<-by(weekday_subset_transactional_baased[,1],weekday_subset_transactional_baased[,2],unique)
count_the_number_of_sms_perday_weekdays_transactionals<-data.frame(sapply(unique_sms_per_day_weekdays_transactionals,length))

average_transactional_sms_weekdays<-round(sum(count_the_number_of_sms_perday_weekdays_transactionals[,1])/nrow(count_the_number_of_sms_perday_weekdays_transactionals))

transactional_weekdays_count<-0

if(average_transactional_sms_weekdays>15){
  transactional_weekdays_count<-5
}else if(average_transactional_sms_weekdays>=10){
  transactional_weekdays_count<-3
}else if(average_transactional_sms_weekdays>5 && average_transactional_sms_weekdays<10){
  transactional_weekdays_count<-1
}else{
  transactional_weekdays_count<-0
  
  
}

##############################################


#### all  bank  based transaction  messages .

all_transat_number_index<-na.omit(data.frame(unlist(rm_default(trans_subset_all$body, pattern = "\\b[X|x]+[0-9]{4}\\b|\\*[0-9]{4}+\\s\\b", extract = TRUE))))

colnames(all_transat_number_index)<-"accounts"
all_transat_number_data<-trans_subset_all[row.names(all_transat_number_index),]

###### Finding how many account  number he is having ?

unique_account_numbers<-unique(gsub(".*[X|x]|.*[*]","",all_transat_number_index$accounts)) ## getting all  account number .

kl<-gsub(".*[X|x]|.*[*]","",all_transat_number_index$accounts)

all_transat_number_data$all_accounts<-gsub(".*[X|x]|.*[*]","",all_transat_number_index$accounts)


##############################################################

### getting weekly , monthly , daily , weekdays columns .

all_transat_number_data$week<-lubridate::week(ymd(all_transat_number_data$date))  ### weekly.

all_transat_number_data$month<-lubridate::month(ymd(all_transat_number_data$date)) ### monthly.

all_transat_number_data$daily<-lubridate::day(ymd(all_transat_number_data$date)) ### daily .

all_transat_number_data$weekday<- weekdays(as.Date(all_transat_number_data$date))
#############################################################################################

##############Daily analysis .

daily<-by(all_transat_number_data$body,all_transat_number_data$date,unique)
number_of_days<-data.frame(sapply(daily,length))
number_of_days[number_of_days==0] <- NA
total_daily_msg<-na.omit(number_of_days)
average_message_daily<-round(sum(total_daily_msg)/nrow(total_daily_msg))

#1) if bank based transactional messages per day averages then (total no of bank sms/ total number of days) * 5

Bank_transaction_msg_perday_average<-average_message_daily


#### average debited amount  per day .


############# Rupees  extraction ################


daily_rs<-function(x){
  number_rupee<-stri_extract_first_regex(gsub(",","",all_transat_number_data[x,1]), "(Rs)\\s[0-9]+\\.\\d+|(INR)\\s[0-9]+\\.\\d+|(Rs.)\\s[0-9]+\\.\\d+|(Rs.)\\s[0-9]+|(Rs)\\s[0-9]+|(INR)\\s[0-9]+")
  return(number_rupee)
}

y_daily_rs<-do.call(rbind.data.frame,lapply(seq(all_transat_number_data[,1]),function(k){daily_rs(k)}))

tryCatch({
colnames(y_daily_rs)<-"rupee"

removing_rs<-data.frame(substr(y_daily_rs$rupee, 4,1000))

},error=function(e){})
#####   available balance .

blance_money<-function(x){
  balance_rupp<-na.omit(data.frame(unlist(rm_default(gsub(",","",all_transat_number_data[x,1]), pattern = "(Rs)\\s[0-9]+\\.\\d+|(INR)\\s[0-9]+\\.\\d+|(Rs.)\\s[0-9]+\\.\\d+|(Rs.)[0-9]+|(Rs)\\s[0-9]+|(INR)\\s[0-9]+|(INR|inr)\\s[0-9]+|(Rs|INR|rs|inr)\\s[-][0-9]+\\.[0-9]+", extract = TRUE))))
  available_balance<-balance_rupp[2,]
  
  blnce<-as.numeric(substr(available_balance,4,1000))
  return(blnce)
}

all_aval_bal<-do.call(rbind.data.frame,lapply(seq(all_transat_number_data[,1]),function(k){blance_money(k)}))
# 
all_aval_bal[is.na(all_aval_bal)]<-0

##############################################################################

##### combining amount with the original data .
tryCatch({
all_transat_number_data_rupee<<-na.omit(data.frame(all_transat_number_data,removing_rs,all_aval_bal))


colnames(all_transat_number_data_rupee)[8]<-"amount"
colnames(all_transat_number_data_rupee)[9]<-"available_balance"

all_transat_number_data_rupee[is.na(all_transat_number_data_rupee)]<-0
all_transat_number_data_rupee$amount<-as.numeric(as.character(paste(all_transat_number_data_rupee$amount)))

},error=function(e){
  
  all_transat_number_data_rupee<<-0
  
})
#####################################################

key_debit<-c("debited")
debited_daily<-grepl(key_debit,all_transat_number_data_rupee$body)
debited_daily_data<-all_transat_number_data_rupee[debited_daily,]

daily_debited<-by(debited_daily_data$amount,debited_daily_data$date,unique)
number_of_days_debited<-data.frame(sapply(daily_debited,sum))
number_of_days_debited[number_of_days_debited==0] <- NA
total_daily_debited_msg<-na.omit(number_of_days_debited)

average_debit_amount<-sum(total_daily_debited_msg[,1])/nrow(total_daily_debited_msg[1])

#3) Average Debited amount per Day (Total Debited Amount / Total number of days).

average_debit_amount_perday_average<-average_debit_amount

#### average credited amount  per day .

key_debit<-"CREDITED|credited"
credited_daily<-grepl(key_debit,all_transat_number_data_rupee$body)
credited_daily_data<-all_transat_number_data_rupee[credited_daily,]

daily_credited<-by(credited_daily_data$amount,credited_daily_data$date,unique)
number_of_days_credited<-data.frame(sapply(daily_credited,sum))
number_of_days_credited[number_of_days_credited==0] <- NA
total_daily_credited_msg<-na.omit(number_of_days_credited)
average_amount_credited_daily<-sum(total_daily_credited_msg)/nrow(total_daily_credited_msg)

##4) Average Credited amount per Day (Total Credited Amount / Total number of days).

average_credited_amount_perday_average<-average_amount_credited_daily

########################################################################

################Weekday analysis .
sunday<-which(all_transat_number_data$weekday=="Sunday")
saturday<-which(all_transat_number_data$weekday=="Saturday")

weekday_subset<-all_transat_number_data[-sunday,]
saturday<-which(weekday_subset$weekday=="Saturday")
weekday_subset<-weekday_subset[-saturday,]


weekday<-by(weekday_subset$body,weekday_subset$date,unique)
number_of_weekdays<-data.frame(sapply(weekday,length))
number_of_weekdays[number_of_weekdays==0] <- NA
total_weekday_msg<-na.omit(number_of_weekdays)
average_message_weekday<-round(sum(total_weekday_msg)/nrow(total_weekday_msg))

###2) if bank based transactional messages weekdays averages (total no of bank sms/ total number of weekdays) * 5

frequency_transactional_message_weekday<-average_message_weekday
###########################################################################################

################Monthly Analysis.

#5.Total number of times Amount credited per month if > 3 assign 5 else if > 2 assign 3 else if > 1 assign 1 else 0.

key_credit<-"CREDITED|credited"
credited_monthly<-grepl(key_credit,all_transat_number_data_rupee$body)
credited_monthly_data<-all_transat_number_data_rupee[credited_monthly,]

credited_monthly_data$year_month<-substr(credited_monthly_data$date,1,7) ### correct month taken from date.

monthly_credited<-by(credited_monthly_data$amount,credited_monthly_data$year_month,unique)
number_of_days_credited<-data.frame(sapply(monthly_credited,sum))
number_of_days_credited[number_of_days_credited==0] <- NA
total_monthly_credited_msg<-na.omit(number_of_days_credited)
number_of_message__credited_monthly<-nrow(total_monthly_credited_msg)


##assign points :

amount_credit_month<-0

if(number_of_message__credited_monthly>=3){
  amount_credit_month<-5
}else if(number_of_message__credited_monthly>=2 &&number_of_message__credited_monthly<3){
  amount_credit_month<-3
} else if(number_of_message__credited_monthly>1 && number_of_message__credited_monthly<2){
  amoun_credit_month<-1
}else {
  amount_credit_month<-0
}

Frequency_amount_credited_sms_per_month<-amount_credit_month

####################################################################################################

#6)Total number of times Amount debited per month if > 10 assign 5 else if > 7 assign 3 else if > 4 assign 1 else 0.



key_debit<-c("debited")
debited_monthly<-grepl(key_debit,all_transat_number_data_rupee$body)
debited_monthly_data<-all_transat_number_data_rupee[debited_monthly,]

debited_monthly_data$year_month<-substr(debited_monthly_data$date,1,7) ### correct month taken from date.


monthly_debited<-by(debited_monthly_data$amount,debited_monthly_data$year_month,unique)
number_of_days_debited<-data.frame(sapply(monthly_debited,length))
number_of_days_debited[number_of_days_debited==0] <- NA
total_monthly_debited_msg<-na.omit(number_of_days_debited)

number_of_debit_amount<-nrow(total_monthly_debited_msg[1])

Frequency_amount_debited_sms_per_month<-number_of_debit_amount
#######################################################################################

##7) Avg Amount Available before 7th of every month if > 20000 assign 5 else if > 15000 assign 4 else if > 10000 assign 3 else if > 5000 assign 2 else if > 3000 assign 1 else 0.


total_summary<-all_transat_number_data_rupee
year<- substr(total_summary$date,1,4)
day<-substr(total_summary$date,9,12)
total_summary$year_month<-paste(year,day,sep="-")
total_summary$index<-seq(nrow(total_summary))


if("7"%in%total_summary$daily){
  
  seventh_of_month_data<-subset(total_summary,daily=="7")  ### taking out 7th of every month.
  
  subset_seventh<-seventh_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum(subset_seventh$available_balance)/nrow(subset_seventh)
  
} else if ("6"%in%total_summary$daily){
  sixth_of_month_data<-subset(total_summary,daily=="6")  ### taking out 6th of every month.
  
  subset_sixth<-sixth_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum( subset_sixth$available_balance)/nrow (subset_sixth)
  
}else if ("5"%in%total_summary$daily){
  fifth_of_month_data<-subset(total_summary,daily=="5")  ### taking out 4th of every month.
  
  subset_fifth<-fifth_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum( subset_fifth$available_balance)/nrow (subset_fifth)
  
}else if ("4"%in%total_summary$daily){
  fourth_of_month_data<-subset(total_summary,daily=="4")  ### taking out 4th of every month.
  
  subset_fourth<-fourth_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum( subset_fourth$available_balance)/nrow (subset_fourth)
  
}else if ("3"%in%total_summary$daily){
  third_of_month_data<-subset(total_summary,daily=="3")  ### taking out 3th of every month.
  
  subset_third<-third_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum( subset_third$available_balance)/nrow (subset_third)
  
}else if ("2"%in%total_summary$daily){
  secound_of_month_data<-subset(total_summary,daily=="2")  ### taking out 2nd of every month.
  
  subset_secound<-secound_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum( subset_secound$available_balance)/nrow (subset_secound)
  
}else if ("1"%in%total_summary$daily){
  first_of_month_data<-subset(total_summary,daily=="1")  ### taking out 1st of every month.
  
  subset_secound<-first_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum( first_secound$available_balance)/nrow (subset_secound)
  
}else if ("8"%in%total_summary$daily){
  eight_of_month_data<-subset(total_summary,daily=="8")  ### taking out 8th of every month.
  
  subset_eight<-eight_of_month_data%>% group_by(date) %>% slice(which.max(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum( subset_eight$available_balance)/nrow (subset_eight)
  
}else if ("9"%in%total_summary$daily){
  ninth_of_month_data<-subset(total_summary,daily=="8")  ### taking out 9th of every month.
  
  subset_ninth<-ninth_of_month_data%>% group_by(date) %>% slice(which.max(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum( subset_ninth$available_balance)/nrow (subset_ninth)
  
}else if ("10"%in%total_summary$daily){
  tenth_of_month_data<-subset(total_summary,daily=="10")  ### taking out 10th of every month.
  
  subset_tenth<-tenth_of_month_data%>% group_by(date) %>% slice(which.max(available_balance))
  
  Average_amount_everymonth_before_seventh<-sum( subset_tenth$available_balance)/nrow (subset_tenth)
  
}


bal_before_svnth<-0

if(Average_amount_everymonth_before_seventh>=20000){
  bal_before_svnth<-5
  
}else if(Average_amount_everymonth_before_seventh>15000 & Average_amount_everymonth_before_seventh < 20000){
  bal_before_svnth<-4
}else if(Average_amount_everymonth_before_seventh>10000 & Average_amount_everymonth_before_seventh< 15000){
  bal_before_svnth<-3
}else if (Average_amount_everymonth_before_seventh>5000& Average_amount_everymonth_before_seventh < 10000){
  bal_before_svnth<-2
}else if (Average_amount_everymonth_before_seventh>3000& Average_amount_everymonth_before_seventh < 5000){
  bal_before_svnth<-1
}else {
  bal_before_svnth<-0
  
}

Available_balance_before_seventh_point<- bal_before_svnth

# ####################################################################################################################
# 
#8) Avg Amount Available before 15th of every month if > 15000 assign 5 else if > 10000 assign 4 else if > 5000 assign 3 else if > 3000 assign 1 else 0.


total_summary<-all_transat_number_data_rupee
year<- substr(total_summary$date,1,4)
day<-substr(total_summary$date,9,12)
total_summary$year_month<-paste(year,day,sep="-")
total_summary$index<-seq(nrow(total_summary))

Average_amount_everymonth_before_fifteen<-0

if("15"%in%total_summary$daily){
  
  fifteen_of_month_data<-subset(total_summary,daily=="15")  ### taking out 15th of every month.
  
  subset_fifteen<-fifteen_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum(subset_fifteen$available_balance)/nrow(subset_fifteen)
  
} else if ("14"%in%total_summary$daily){
  fourteen_of_month_data<-subset(total_summary,daily=="6")  ### taking out 14h of every month.
  
  subset_fourteen<-fourteen_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( subset_fourteen$available_balance)/nrow (subset_fourteen)
  
}else if ("13"%in%total_summary$daily){
  thirteen_of_month_data<-subset(total_summary,daily=="13")  ### taking out 13th of every month.
  
  subset_thirteen<-thirteen_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( subset_thirteen$available_balance)/nrow (subset_thirteen)
  
}else if ("12"%in%total_summary$daily){
  twelve_of_month_data<-subset(total_summary,daily=="12")  ### taking out 12th of every month.
  
  subset_twelve<-twelve_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( subset_twelve$available_balance)/nrow (subset_twelve)
  
}else if ("11"%in%total_summary$daily){
  eleventh_of_month_data<-subset(total_summary,daily=="11")  ### taking out 11th of every month.
  
  subset_eleventh<-eleventh_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( subset_eleventh$available_balance)/nrow (subset_eleventh)
  
}else if ("10"%in%total_summary$daily){
  tenth_of_month_data<-subset(total_summary,daily=="10")  ### taking out 2nd of every month.
  
  subset_tenth<-tenth_of_month_data%>% group_by(date) %>% slice(which.min(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( subset_tenth$available_balance)/nrow (subset_tenth)
  
}else if ("16"%in%total_summary$daily){
  sixteen_of_month_data<-subset(total_summary,daily=="16")  ### taking out 16th of every month.
  
  subset_sixteen<-sixteen_of_month_data%>% group_by(date) %>% slice(which.max(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( sixteen_secound$available_balance)/nrow (subset_sixteen)
  
}else if ("17"%in%total_summary$daily){
  seventeen_of_month_data<-subset(total_summary,daily=="17")  ### taking out 8th of every month.
  
  subset_seventeen<-seventeen_of_month_data%>% group_by(date) %>% slice(which.max(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( subset_seventeen$available_balance)/nrow (subset_seventeen)
  
}else if ("18"%in%total_summary$daily){
  eighteen_of_month_data<-subset(total_summary,daily=="18")  ### taking out 9th of every month.
  
  subset_eighteen<-eighteen_of_month_data%>% group_by(date) %>% slice(which.max(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( subset_eighteen$available_balance)/nrow (subset_eighteen)
  
}else if ("19"%in%total_summary$daily){
  ninteen_of_month_data<-subset(total_summary,daily=="19")  ### taking out 19th of every month.
  
  subset_ninteen<-ninteen_of_month_data%>% group_by(date) %>% slice(which.max(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( subset_ninteen$available_balance)/nrow (subset_ninteen)
  
}else if ("20"%in%total_summary$daily){
  twenty_of_month_data<-subset(total_summary,daily=="20")  ### taking out 20th of every month.
  
  subset_twenty<-twenty_of_month_data%>% group_by(date) %>% slice(which.max(available_balance))
  
  Average_amount_everymonth_before_fifteen<-sum( subset_twenty$available_balance)/nrow (subset_twenty)
  
}


bal_before_fifteen<-0

if(Average_amount_everymonth_before_fifteen>=20000){
  bal_before_fifteen<-5
  
}else if(Average_amount_everymonth_before_fifteen>15000 & Average_amount_everymonth_before_fifteen < 20000){
  bal_before_fifteen<-4
}else if(Average_amount_everymonth_before_fifteen>10000 & Average_amount_everymonth_before_fifteen < 15000){
  bal_before_fifteen<-3
}else if (Average_amount_everymonth_before_fifteen>5000& Average_amount_everymonth_before_fifteen < 10000){
  bal_before_fifteen<-2
}else if (Average_amount_everymonth_before_fifteen>3000& Average_amount_everymonth_before_fifteen < 5000){
  bal_before_fifteen<-1
}else {
  bal_before_fifteen<-0
  
}

Available_balance_before_fifteen_point<-  bal_before_fifteen
#############################################################################################


###### weekly analysis :


####debit  analysis ##############
debited_data<-debited_daily_data
debited_data$year_month<-substr(debited_data$date,1,7)
credited_data<-credited_daily_data
credited_data$year_month<-substr(credited_data$date,1,7)


weekwise_aggregation<-debited_data%>%select(year_month,week,amount,available_balance)

library(dplyr)
weekwise_average<-weekwise_aggregation %>%group_by(year_month,week) %>%summarise_each(funs(mean), amount, available_balance)


##########credit analysis ################

credited_data<-credited_daily_data
credited_data$year_month<-substr(credited_data$date,1,7)
credited_data<-credited_daily_data
credited_data$year_month<-substr(credited_data$date,1,7)


weekwise_aggregation_credited<-credited_data%>%select(year_month,week,amount,available_balance)

library(dplyr)
weekwise_average_credited<-weekwise_aggregation_credited %>%group_by(year_month,week) %>%summarise_each(funs(mean), amount, available_balance)

###9) Bank Transfer Amount each month if > 15000 assign 5 else if > 10000  assign 4 else if > 5000 assign 3 else if > 3000 assign 1 else 0 .

credited_monthly_data_balance<-mean(credited_monthly_data$available_balance)


bank_transfer_amount<-0

if( credited_monthly_data_balance>15000){
  bank_transfer_amount<-5
}else if (credited_monthly_data_balance>10000 & credited_monthly_data_balance < 15000){
  bank_transfer_amount<-4
}else if(credited_monthly_data_balance>5000 & credited_monthly_data_balance<10000){
  bank_transfer_amount<-3
}else if(credited_monthly_data_balance>3000 & credited_monthly_data_balance< 5000){
  bank_transfer_amount<-1
}else{
  bank_transfer_amount<-0
}

Frequency_of_bank_transfer_each_month<- bank_transfer_amount
################################more to  be added #####################################

sms_based_analysis<<-0
sms_based_analysis<<-data.frame(paste("user",sms,sep="_"),Frequency_contact_based_count_perday,Frequency_contact_based_count_weekly,Frequency_promotional_based_sms_weekly,Frequency_promotional_based_sms_per_day,transactional_weekdays_count,Frequency_count_Transactional_based_daily,Bank_transaction_msg_perday_average,frequency_transactional_message_weekday,average_debit_amount_perday_average,
                          
                          average_credited_amount_perday_average,Frequency_amount_credited_sms_per_month,Frequency_amount_debited_sms_per_month,
                          Available_balance_before_seventh_point,Available_balance_before_fifteen_point, Frequency_of_bank_transfer_each_month )
  }, error=function(e){

    sms_based_analysis<<-data.frame(paste("user",sms,sep="_"),Frequency_contact_based_count_perday=0,Frequency_contact_based_count_weekly=0,Frequency_promotional_based_sms_weekly=0,Frequency_promotional_based_sms_per_day=0,transactional_weekdays_count=0,Frequency_count_Transactional_based_daily=0,Bank_transaction_msg_perday_average=0,frequency_transactional_message_weekday=0,average_debit_amount_perday_average=0,

                               average_credited_amount_perday_average=0,Frequency_amount_credited_sms_per_month=0,Frequency_amount_debited_sms_per_month=0,
                               Available_balance_before_seventh_point=0,Available_balance_before_fifteen_point=0, Frequency_of_bank_transfer_each_month=0 )

  })

  return( sms_based_analysis)

}
sms_analysis<-data.frame()

sms_scoring_results<-do.call(rbind.data.frame,lapply(seq(call_S3_character),function(k){sms_data_analysis(k) }))
colnames(sms_scoring_results)[1]<-"users"


sms_scoring_results<-cbind(id,sms_scoring_results)

########################################################################################

library(RMongo)
library(rmongodb)
library(mongolite)

c = mongo(collection = "sms_scoring_results", db = "sms_data") ## Creating/using db and to make a connection .
c$insert(sms_scoring_results) ## insert data  into the collection "y".  


