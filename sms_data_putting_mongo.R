

library(dplyr)
library(aws.s3)
library(RJSONIO)
library(plyr)
library(stringi)
library(stringr)
library(lubridate)
library(stringi)

library(stringr)
library(lubridate)
library(dplyr)
library(qdapRegex)
# ########################################
# ######################## Fetching  Data from S3 database.
Sys.setenv("AWS_ACCESS_KEY_ID" ='AKIAIKOI5YSEVXZ3LVNQ',  
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
} ###Function to  fetch data  from S3 .

files <- get_bucket(bucket = 'wallet1234')  #### bucket  details .

rel<-data.frame(do.call("rbind",files))  ###  function  to  bind all  S3  files .
dat <- lapply(files, function(j) {
  list(replace(j, sapply(j, is.list), NA))
})
key_values<-data.frame(rel$Key)

colnames(key_values) <- as.character(unlist(key_values[1,]))
key_values=key_values[-1,]
sms_S3<-data<-subset(key_values, select = grepl("sms/", names(key_values))) ## Took out  all userid having call.
sms_S3_character<-names(sms_S3) ### userids .

id<- gsub("sms/","",sms_S3_character) ####taking out  userids .
##################################################################################################
###########################S3  function  ends  here ###############################################
###################################################################################################


###################################################################################################
##################Function to update mongodb  with all data's  having aggregation ################
##################as  Transactional , Promotional , Contact based  sms datas ######################
###################################################################################################

all_data_db<-function(db){ ##  function to update mongo db.
  df<-aws_call(sms_S3_character[45])  ### calling  all s3 files .
    df$msg_pattern<-0
    df$msg_type<-0
    df$amount<-0
  
    ###for irctc we  define 5  columns.
    
    df$journey_date<-0
    df$start_destination<-0
    df$name_customer<-0
    df$coach_of_customer<-0
    df$ticket_fare<-0
    
    
    #######for  account based sms .
    
    df$bank_name<-0
    df$transaction_medium<-0
    df$accoun_card_number<-0
    df$amount_value<-0
    df$balance_value<-0
    df$message_transaction_type<-0
    df$deduction_transaction_type<-0
    ###############################################
    
    df_date_time<-as.POSIXct((df$date)/1000,origin="1970-01-01")  ###  converting dates to  date format .
    df_date<-substr(df_date_time,1,11)
    df_time<-substr(df_date_time,11,20)
    df_date_parsed<-cbind(df,df_date_time,df_date,df_time)
    
    df<-df_date_parsed
    trans_subset_all<-data.frame(df$body,df_date) #### Formated datas .
    colnames(trans_subset_all)<-c("body","date")
  
 
    #########################################################################################
    # ##### How many  sms recieved per day .
    df_date_parsed$day<-day(df_date_parsed$df_date)
    # quantity_of_sms_per_day<-by(df_date_parsed$address,df_date_parsed$df_date,unique)
    # finding_number_of_counts_sms<-data.frame(sapply(quantity_of_sms_per_day,length))
    # row.names(finding_number_of_counts_sms)<-NULL
    # average_number_of_sms<- round(mean(finding_number_of_counts_sms[,1]))
    # 
    # #######################################################################################
    
    
    # Deviding data into contact based , promotional ,transactional on the basis of matching strings .
    
    ###################################################################################
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
    
    ###Taking out Contact based  data  from  the  data.################################
    
    df_date_parsed_address<-data.frame(df_date_parsed$address,df_date_parsed$body,df_date_parsed$df_date,df_date_parsed$day) 
    df_date_parsed_address[,1]<-as.character(df_date_parsed_address[,1])
    count_t<-nchar(df_date_parsed_address[,1])
    datafrme<-cbind(df_date_parsed_address,count_t)
    less_than_10<-datafrme%>%subset(as.numeric(datafrme[,5])< 10)  ### checking size of phone number less  than  10 in address  column.
    greater_than_10<-datafrme%>%subset(as.numeric(datafrme[,5])>= 10)  ### checking size of phone number  greater than  10 in address  column..
    taking_out_character_values<-grep("[A-Z,a-z]",greater_than_10[,1]) ### if character values there  in address  section   take it out and merge it with  promotional/transactional.
    
    
    sorted_character<-greater_than_10[taking_out_character_values,] #### taking out  all  character value datas  in address  section .
    
    
    binding_characters_from_other_numeric_data<-rbind(less_than_10,sorted_character) ## promotional and transactional data .
    
    if(length( taking_out_character_values)!=0){ ### checking if charater value exist in address section delete  those rows .
    contact_based_values<-greater_than_10[-taking_out_character_values,] 
    }else{                             ####  if character  value  dont exist  then it means all values  in address  section are  numeric/contact based data .
      contact_based_values<-greater_than_10
    }
    all_contact_based_data<-contact_based_values  #### final  contact  based  data  without  any  character  values  in adress  section .
    

  ###################################################################################
  ############All  contact  based  datas  extraction  ends  here  #####################
   
  #################################################################################### 
  ###### tranactional messages taking out #############################################
  
  lowercase_body_address<-tolower(binding_characters_from_other_numeric_data[,2]) ### taking out  from  top module , all  promotional and transactional datas  converting into  lowercase to analyse .
   sorting_array<- tolower(c("Service provider","MB","121", "121","Recharge", "Unlimited", "offer", "3G"," 4G", "2G", "Dial", "28 days"," GB"," Calls", "Talktime", "Free"," TT", " in one word etc"," 0ff","%", "Cashback", "Fees", "account talktime", "Voucher", "Valid", "today", "Customer", "local+std", "tollfree", "network","toll-free")) ### keywords  to check in the  body section for promotional messages .
   
    put_address<-length(lowercase_body_address) ## getting length  of the sorted  address section containing  prom and trans msg.

    get_address<-length(sorting_array) ### finding  length  of the sort keys .
    output_address<-matrix(ncol=get_address,nrow=put_address) ### matrix  to  find  out the  distance  betwn keywrds and adress section .
  
    tryCatch({
      for(i in 1:put_address)  ### function to find out  the count  of  promotionals .
      {
        
        output_address[,i]<- str_count(lowercase_body_address,sorting_array[i])
        
      }
      
    }, error=function(e){})
    sum_thecount_address<-data.frame(rowSums(output_address))  ##  sum  up the counts  
    
    matching_count_address_result<-cbind(binding_characters_from_other_numeric_data[,1],binding_characters_from_other_numeric_data[,2],binding_characters_from_other_numeric_data[,3],binding_characters_from_other_numeric_data[,4],binding_characters_from_other_numeric_data[,5],sum_thecount_address) ## finding the count of the string matching .
    
    promotional_msg_count_address_analysis<-matching_count_address_result%>%subset(matching_count_address_result[,6]>3) ## finding out the matches more than 3 three  then we r tlking it as proms .
    
    left_trans_promotional_msg<-matching_count_address_result[-as.numeric(row.names(promotional_msg_count_address_analysis)),]  ### deleting all  the proms  from the original data contn trans and proms .
    
    numeric_value_deletion<-grep("[0-9]",left_trans_promotional_msg[,1])  ### finding out if any phone numbers/numeric values  present or not in the address  section .
    
    data_numeric_value_deletion<-left_trans_promotional_msg[numeric_value_deletion,] ### deleting if any  numeric value/ phone number present .
    
    left_trans_promotional_msg_numeric_removel<-left_trans_promotional_msg[-numeric_value_deletion,] ### deleting all numeric value from the original address section .
    left_trans_promotional_msg_numeric_removel[,2]<-tolower(left_trans_promotional_msg_numeric_removel[,2]) ### removed column to  be merged into  original section .
    
    
    #########################################################################################################################################
    ########################################Finding out all  transactional datas from the  original data  containing  proms and trans .
    #########################################################################################################################################
    
    matching_body_transactional<-"a/c|account|ac|xxxx|bal|debit|debited|recharge|txn|available|balance|rs.|your account|towards|cash withdrawal|atm withdrawals|withdrawals|amount of INR|An amount|Avail.|Bal| bank|  a/c|  refunded|your account no| pos|pos withdrawal| recharge done|Transid| mrp| txn id|initiated|available balance|online recharge txn id|debited with|otp|verify mobile|your otp|Rs|sb| gone below| minimum balance|order|your order|successfully placed|will be delivered|EPF balance|Amt:Rs.|Amt|Account updated|bookingno|delivered shortly|top up recharge successful|ola! Your bill for| Distance:|Time:|your ola| recharge of rs.|order id|recharge successful| avl bal|ECS|NACH| towards ECS|avoid penalty| monthly avg|monthly average| min balance|minimum balance|previous not maintained| one time password|debit card|credit card|credit| bank debit| bank credit|pos transaction|top up|refund|wallet"
    matching_body_transactional<-"rs|inr"
    
    strng_match<-grepl(matching_body_transactional,left_trans_promotional_msg_numeric_removel[,2])  ### finding out the matching keywrds in the  address section for trans .
    
    strng_match_transaction_final<-left_trans_promotional_msg_numeric_removel[strng_match,] ### taking out all  the  rows  matching the above  keywords as transactionals .
  
    ##########################################################################################
    #########################transactional  data  extraction ends here ########################
    ##########################################################################################
  
    
    #### all  transactional datas  naming . 
    tryCatch({
    all_transactions<-df[row.names( strng_match_transaction_final),]  ##### getting the rownumbers  of the  matching transactional data in the original dataset  (df).
    all_transactions$msg_pattern<-"Transactions"  #####matching rownames of the transactional data are to  be flagged as  transactional .
    
    }, error=function(e){})
  
    #### all  contact based datas naming .
   tryCatch({
    all_contact_based<-df[row.names(all_contact_based_data),]  #### getting the rownames from the 1st module having  all  numeric/phone number data .
    all_contact_based$msg_pattern<-"Contacts"  #### matching rownames  of the contact based data are flagged as contacts .
    
    }, error=function(e){})
    
  
    df[row.names(all_transactions),]<-all_transactions #########replace  in the original dataset df with the modified transactional data using rownames .
    df[row.names(all_contact_based),]<-all_contact_based #########replace  in the original dataset df with the modified contact data using rownames .
    
   trk<- df$msg_pattern
      
   ### Rest  all  are promotionals data .
    trk[which(trk=="0")]<-"Promotional"   #######Flagging  rest  all  the  datas  as  promotional datas .
    df$msg_pattern<-trk 
    
    
    ########################################################################################################################################################
    #############Filling messsage type of  transactional data  and amount  #################################################################################
    
    ## 1. Filtering transactional data by  recharges and online transactions  .
    
    keywords_expenditure<-"recharge"
    body_of_all_transactions<-all_transactions$body ##### extracting body part of transaction data.
    
    
    expenditure_based_datas_extraction<-grepl(keywords_expenditure,tolower(all_transactions$body)  )### knowing the msg from body having this keywords.
    
    expenditure_datas<-all_transactions[expenditure_based_datas_extraction,] #### extracting the data having the keywords.
    
    keywords_amount<-"done|successful|success|process|mrp|balance|txn|trans|transid|tranc id"
    
    amount_recharge<-grepl(keywords_amount,tolower(expenditure_datas$body))
    
    amount_recharge_data<-expenditure_datas[amount_recharge,]
    amount_recharge_data$msg_type <-"recharges" #### changing those msg with another column with expenditure word.
    
    #########################################################
    ####taking out the amount(rs/inr) from the body which is having rs/inr in there  body .
    #########################################################
     target_amount<-rm_default(pattern="(rs|rs\\.|rs\\s|rs.\\s|inr|inr\\s)[0-9]+",tolower(amount_recharge_data$body),extract = T)
    target_amount[is.na(target_amount)]<-0
    extract_money <-do.call(cbind, target_amount)
    amount<-as.numeric(gsub("(rs|rs\\.|rs\\s|rs.\\s|inr|inr\\s)","",extract_money[1,]))
    amount_recharge_data$amount<-amount
    
    #########################################################################################################
    ##################################Targeting unique recharge messages 
    #################################################################################
    duplicate_recharge<-function(x){
      
    e<-amount_recharge_data$amount
    kl<-which((e[x]==e[])==TRUE)
    
    getting_values<-amount_recharge_data[kl,]
    
    if(getting_values$msg_type=="recharges"){
    options("scipen"=100, "digits"=4) ###converting  exponential to  digits .
   time_in_hours<- difftime(getting_values$df_date_time[1],getting_values$df_date_time[],units="hours")
   
   getting_values$time_diff<-time_in_hours
   
  subst_data<- subset(getting_values,getting_values$time_diff<2)
  
 duplicated_values<-subst_data[-1,]
 if(nrow(duplicated_values)==0){
   duplicated_values
 }else{
 getting_values[row.names(duplicated_values),]$msg_type<-"recharges_duplicated"
 }
 
 return(getting_values)
      }
      else{
        return(getting_values)
      }
    }
   
    duplicate_recharge_amount<-do.call(rbind.data.frame,lapply( seq(nrow(amount_recharge_data)),function(k){duplicate_recharge(k) }))
    
    unique_recharge_values<-  duplicate_recharge_amount[!duplicated( duplicate_recharge_amount),] 
    unique_recharge_values<-unique_recharge_values[,1:length(unique_recharge_values)-1]
    
    df[row.names( unique_recharge_values),]<-unique_recharge_values
    
    ##################################################################################
    #####################recharge based transaction ends here ########################
    
    #################################################################################
    ##################Food based transactional results starts 
    #################################################################################
    
    
    online_shop<-"foodpanda|swiggy|dominos|LBB's Loving|chefkraft|brekkie|Dropkaffe|Chefkraft|Meal Diaries|Fresh Menu|Masala Box|Red Cooker|orange oak|sangeeta|saleem rr biryani|zomato|comesum|box8.in|venegar|rads poha|readybowl|fassos|spoonjoy|slurpyn|eatongo|bhukkad|cookaroo|fitgo|eatfresh|eatlo|brekkie|dazo|hungerbox|box8|dropkafee|chai point|quinto|chefhost|medine|momoe|dineout|bigbasket|halfteaspoon|eatlo|tapcibo|tinyowl|yumist"
    
    body_of_all_transactions_food<-all_transactions$body ##### extracting body part of transaction data.
    
    
    expenditure_based_datas_extraction_food<-grepl(tolower(online_shop),tolower(body_of_all_transactions_food)  )### knowing the msg from body having this keywords.
    
    expenditure_datas_food<-all_transactions[expenditure_based_datas_extraction_food,] #### extracting the data having the keywords.
    
   # keywords_amount_food<-"order no|delivered shortly"
  
   # amount_recharge_food<-grepl(keywords_amount_food,tolower(expenditure_datas_food$body))
    
   # amount_recharge_data_food<-expenditure_datas_food[amount_recharge_food,]
   # amount_recharge_data_food$msg_type <-"food" #### changing those msg with another column with expenditure word.
    expenditure_datas_food$msg_type<-"food"
    #########################################################
    ####taking out the amount(rs/inr) from the body which is having rs/inr in there  body .
    #########################################################
    target_amount<-rm_default(pattern="(rs|rs\\.|rs\\s|rs.\\s|inr|inr\\s)[0-9]+",tolower( expenditure_datas_food$body),extract = T)
    target_amount[is.na(target_amount)]<-0
    extract_money <-do.call(cbind, target_amount)
    amount<-as.numeric(gsub("(rs|rs\\.|rs\\s|rs.\\s|inr|inr\\s)","",extract_money[1,]))
    expenditure_datas_food$amount<-amount
    
    amount_food_data<-expenditure_datas_food
    #########################################################################################################
    ##################################Targeting unique recharge messages 
    #################################################################################
    duplicate_food<-function(x){
      
      all_food<-amount_food_data$amount
      individual_food<-which((all_food[x]==all_food[])==TRUE)
      
      getting_values_food<-amount_food_data[  individual_food,]
      
      if(getting_values_food$msg_type=="food"){
        options("scipen"=100, "digits"=4) ###converting  exponential to  digits .
        time_in_hours_food<- difftime(getting_values_food$df_date_time[1],getting_values_food$df_date_time[],units="hours")
        
        getting_values_food$time_diff<-time_in_hours_food
        
        subst_data_food<- subset(getting_values_food,getting_values_food$time_diff<2)
        
        duplicated_values_food<-subst_data_food[-1,]
        if(nrow(duplicated_values_food)==0){
          duplicated_values_food
        }else{
          getting_values_food[row.names(duplicated_values_food),]$msg_type<-"food_duplicated"
        }
        
        return(getting_values_food)
      }
      else{
        return(getting_values_food)
      }
    }
    
    duplicate_amount_food<-do.call(rbind.data.frame,lapply( seq(nrow( expenditure_datas_food)),function(k){duplicate_food(k) }))
    
    unique_values_food<-  duplicate_recharge_amount_food[!duplicated( duplicate_amount_food),] 
    unique_values_food<-unique_values_food[,1:length(unique_values_food)-1]
    
    df[row.names( unique_values_food),]<-unique_values_food
    
    
    ###############################################################################################################################################
    #####################Food  based transactional messages  ends here
    #####################################################################################################
    
    ###################################################################
    ###################Travel  based transactional messages starts 
    ###################################################################
    ####service providers are : "ola" ,"uber" ....
    
    
    ###################
    ##ola  based messages.
    
    remove_numrc_addrs<-grep("[0-9]",all_transactions$address) ### removing numeric values from address.
    
    trans_addrs<-all_transactions[-remove_numrc_addrs,] ## getting datas .
    
    olacabs_msg<-trans_addrs[stri_sub(trans_addrs$address,4,12)=="OLACBS",] ### all  ola messages .
    
    ##list_of_cab_providers<-c("ola","uber","meru","taxiforsure","yellowcabs","easycabs","savarii","utoo")
   
    online_travel_ola<-"total bill|ola money account|credited|pay bill|transaction id"
    
    body_of_all_transactions_travel_ola<-olacabs_msg$body ##### extracting body part of transaction data.
    
    expenditure_based_datas_extraction_travel_ola<-grepl(tolower(online_travel_ola),tolower(body_of_all_transactions_travel_ola)  )### knowing the msg from body having this keywords.
    
    expenditure_datas_travel_ola<-all_transactions[expenditure_based_datas_extraction_travel_ola,] #### extracting the data having the keywords.
    
    if( !nrow(expenditure_datas_travel_ola)){
      olacabs_msg$msg_type<-"travel_prom_ola"
    }else{
      olacabs_msg$msg_type<-"travel_ola"
    }
    
    ###################################################################################
    ####taking out the amount(rs/inr) from the body which is having rs/inr in there  body .
    #########################################################
    target_amount_travel_ola<-rm_default(pattern="(rs|rs\\.|rs\\s|rs.\\s|inr|inr\\s)[0-9]+",tolower( olacabs_msg$body),extract = T)
    target_amount_travel_ola[is.na(target_amount_travel_ola)]<-0
    extract_money_travel_ola <-do.call(cbind, target_amount_travel_ola)
    amount_travel_ola<-as.numeric(gsub("(rs|rs\\.|rs\\s|rs.\\s|inr|inr\\s)","",extract_money_travel_ola[1,]))
   olacabs_msg$amount<-amount_travel_ola
    amount_travel_data_ola<- olacabs_msg
    
    ##################################################################################################
    
    ##################################Targeting unique travel messages 
    #################################################################################
    duplicate_travel_ola<-function(x){
      
      all_travel_ola<-amount_travel_data_ola$amount
      individual_travel_ola<-which((all_travel_ola[2]==all_travel_ola[])==TRUE)
      
      getting_values_travel_ola<- amount_travel_data_ola[  individual_travel_ola,]
      
      if(getting_values_travel_ola$msg_type=="travel_ola"){
        options("scipen"=100, "digits"=4) ###converting  exponential to  digits .
        time_in_hours_travel_ola<- difftime(getting_values_travel_ola$df_date_time[1],getting_values_travel_ola$df_date_time[],units="hours")
        
        getting_values_travel_ola$time_diff<-time_in_hours_travel_ola
        
        subst_data_travel_ola<- subset(getting_values_travel_ola,getting_values_travel_ola$time_diff<2)
        
        duplicated_values_travel_ola<-subst_data_travel_ola[-1,]
        if(nrow(duplicated_values_travel_ola)==0){
          duplicated_values_travel_ola
        }else{
          getting_values_travel_ola[row.names(duplicated_values_travel_ola),]$msg_type<-"travel_duplicated_ola"
        }
        
        return(getting_values_travel_ola)
      }
      else{
        
        getting_values_travel_ola$msg_type=="travel_prom_ola"
        return(getting_values_travel_ola)
      }
    }
    
    duplicate_amount_travel_ola<-do.call(rbind.data.frame,lapply( seq(nrow(olacabs_msg)),function(k){duplicate_travel_ola(k) }))
    
    unique_values_travel_ola<- amount_travel_data_ola[!duplicated(duplicate_amount_travel_ola),] 
    unique_values_travel_ola<-unique_values_travel_ola[,1:length(unique_values_travel_ola)-1]
    
    df[row.names( unique_values_travel_ola),]<-unique_values_travel_ola
    
    
    
    
    
    ##################ola analysis ends here .
    ######################################################################## 
    
    
    ############################
    ##other cab providers messages ("uber","meru","taxiforsure","yellowcabs","easycabs","savarii","utoo")
    ############################
    online_travel_others<-c("uber","meru","taxiforsure","yellowcabs","easycabs","savarii","utoo")
    
    other_cab<-function(i){
    
    body_of_all_transactions_travel_others<-all_transactions$body ##### extracting body part of transaction data.
    
    expenditure_based_datas_extraction_travel_others<-grepl(tolower(online_travel_others[1]),tolower(body_of_all_transactions_travel_others)  )### knowing the msg from body having this keywords.
    
    expenditure_datas_travel_others<-all_transactions[expenditure_based_datas_extraction_travel_others,] #### extracting the data having the keywords.
    
    expenditure_datas_travel_others$msg_type<-paste("travel","prom",online_travel_others[i],sep="_")
    
   
    
    ###################################################################################
    ####taking out the amount(rs/inr) from the body which is having rs/inr in there  body .
    #########################################################
    target_amount_travel_others<-rm_default(pattern="(rs|rs\\.|rs\\s|rs.\\s|inr|inr\\s)[0-9]+",tolower( expenditure_datas_travel_others$body),extract = T)
    target_amount_travel_others[is.na(target_amount_travel_others)]<-0
    extract_money_travel_others <-do.call(cbind, target_amount_travel_others)
    amount_travel_others<-as.numeric(gsub("(rs|rs\\.|rs\\s|rs.\\s|inr|inr\\s)","",extract_money_travel_others[1,]))
    expenditure_datas_travel_others$amount<-amount_travel_others
    amount_travel_data_others<-expenditure_datas_travel_others
    
    ##################################################################################################
    
    ##################################Targeting unique travel messages 
    #################################################################################
    duplicate_travel_others<-function(x){
      
      all_travel_others<-amount_travel_data_others$amount
      individual_travel_others<-which((all_travel_others[x]==all_travel_others[])==TRUE)
      
      getting_values_travel_others<-amount_food_travel_others[  individual_travel_others,]
      
      if(getting_values_travel_others$msg_type==paste("travel","prom",online_travel_others[i],sep="_")){
        options("scipen"=100, "digits"=4) ###converting  exponential to  digits .
        time_in_hours_travel_others<- difftime(getting_values_travel_others$df_date_time[1],getting_values_travel_others$df_date_time[],units="hours")
        
        getting_values_travel_others$time_diff<-time_in_hours_travel_others
        
        subst_data_travel_others<- subset(getting_values_travel_others,getting_values_travel_others$time_diff<2)
        
        duplicated_values_travel_others<-subst_data_travel_others[-1,]
        if(nrow(duplicated_values_travel_others)==0){
          duplicated_values_travel_others
        }else{
          getting_values_travel_others[row.names(duplicated_values_travel_others),]$msg_type<-paste("travel","prom",online_travel_others[i],"duplicated",sep="_")
        }
        
        return(getting_values_travel_others)
      }
      else{
        return(getting_values_travel_others)
      }
    }
    
    duplicate_amount_travel_others<-do.call(rbind.data.frame,lapply( seq(nrow( expenditure_datas_travel_others)),function(k){duplicate_travel_others(k) }))
    
    unique_values_travel_others<-  duplicate_amount_travel[!duplicated( duplicate_amount_travel_others),] 
    unique_values_travel_others<-unique_values_travel[,1:length(unique_values_travel_others)-1]
    
    df[row.names( unique_values_travel_others),]<-unique_values_travel_others
    
    return(df)
    }
    
    
    other_cabs_data_type<-lapply(seq( online_travel_others),function(k){tryCatch({ other_cab(k)
      
      
    },error=function(e){})
    })
    #############################################################################################
    ############################Travel  based transactions ends here .
    ###############################################################################################
    
    
    ##########################################################################################
    ########################irctc data analysis .
    
    df_train<-df
    ##irctc  based messages.
    
    remove_numrc_addrs_irctc<-grep("[0-9]",df_train$address) ### removing numeric values from address.
    
    trans_addrs_irctc<- df_train[-remove_numrc_addrs_irctc,] ## getting datas .
    
    irctc_msg<-subset(trans_addrs_irctc,stri_sub(trans_addrs_irctc$address,4,12)=="IRCTCi") ### all  irctc messages .
    
    train_keywrds<-"dep|fare"
    
    train_details<-irctc_msg[grepl(tolower(train_keywrds),tolower(irctc_msg$body)),]
    
    train_details_body<-tolower(train_details$body)
    
    splitted_string<-strsplit(train_details_body,split=',')
    splitted_string_frame<-do.call(cbind,splitted_string)
    
    
    journey_details<-function(x){
    journey_date<-splitted_string_frame[3,x]
    journey_date<-strsplit(journey_date,split=':')[[1]][2]
    start_destination<-splitted_string_frame[5,x]
    name_customer<-splitted_string_frame[7,x]
    coach_of_customer<-splitted_string_frame[4,x]
    ticket_fare<-splitted_string_frame[9,x]
    ticket_fare<-as.numeric(paste(strsplit(ticket_fare,split=':')[[1]][2]))
    user_irctc_details<-data.frame(journey_date,start_destination,name_customer,coach_of_customer,ticket_fare)
    
    return(user_irctc_details)
    
    }
    
    irctc_for_all_user<-lapply(seq(ncol(splitted_string_frame)),function(k){tryCatch({ journey_details(k)
      
      
    },error=function(e){})
    })  
     
    ####merging results with original data  i.e :- df .
    
    binding_data<-cbind(train_details,data.frame(irctc_for_all_user))
    removing_duplicates<-binding_data[!duplicated(colnames(binding_data),fromLast = T)]
    
    df[row.names(removing_duplicates),]<-removing_duplicates
    
    
    
    ##################### Accounts based transactional details .
    
    account_card_ragex<-"[\\*]+[0-9]+(\\.)+[0-9]+|[0-9]*([X|x]|[\\*])+[0-9]+"
    
    all_transat_number_index<-na.omit(data.frame(unlist(rm_default(df$body, pattern =  account_card_ragex, extract = TRUE))))
    
    
    transaction_data_account<-df[row.names(all_transat_number_index),]

    
    sorting_account<- tolower(c("a/c","account","ac","card")) ### keywords  to check in the  body section for promotional messages .
    
    put_address_account<-length(tolower(transaction_data_account$body)) ## getting length  of the sorted  address section containing  prom and trans msg.
    
    get_address_account<-length(sorting_account) ### finding  length  of the sort keys .
    output_account<-matrix(ncol=get_address_account,nrow=put_address_account) ### matrix  to  find  out the  distance  betwn keywrds and adress section .
    
    tryCatch({
      for(i in 1:put_address_account)  ### function to find out  the count  of  promotionals .
      {
        
        output_account[,i]<- str_count(tolower(transaction_data_account$body),sorting_account[i])
        
      }
      
    }, error=function(e){})
    sum_account<-data.frame(rowSums(output_account))  ##  sum  up the counts  .
    
    ######removing datas  with  no match  or  NA .
    
    sum_account[is.na(sum_account)]<-0 ### converting  NAs into  zeros for removel .
    
    account_remove_zeros<-transaction_data_account[-which(sum_account=="0"),]  ###removing  rows with  no  match or zeros.

    all_transat_number_index_afterstringsplt<-data.frame(str_extract(account_remove_zeros$body, "[\\*]+[0-9]+(\\.)+[0-9]+|[0-9]*([X|x]|[\\*])+[0-9]+"))
    
    row.names(all_transat_number_index_afterstringsplt)<-row.names(account_remove_zeros)

    all_transatsplit_frame<-account_remove_zeros[row.names(na.omit(all_transat_number_index_afterstringsplt)),]
    
    split_trans<-data.frame(str_extract( all_transatsplit_frame$body, "[\\*]+[0-9]+(\\.)+[0-9]+|[0-9]*([X|x]|[\\*])+[0-9]+"))
    
    
    ######################################################################
    #############Function to  calculate account  details of customers through his msg.
   
    breaking_function<-function(x){
      
      tryCatch({
      
    splitted_value_by_ragex_first<-strsplit(as.character(all_transatsplit_frame$body[x]),split=as.character(split_trans[1,1]),fixed=TRUE)[[1]][1]
    
    
    #################taking the amount  section for  ruppes .
    number_rupee<-str_extract_all(all_transatsplit_frame$body[x], "(Rs)(\\s)?[0-9]+(\\.\\d+)?|(INR)(\\s)?[0-9]+(\\.\\d+)?|(Rs.)(\\s)?[0-9]+(\\.\\d+)?|(Rs.)(\\s)?[0-9]+|(Rs)(\\s)?[0-9]+|(INR)(\\s)?[0-9]+|(\\+)[0-9]+(\\.[0-9]+)?")
    
    options(digits = 9)
    amount_value<-as.numeric(str_extract(number_rupee[[1]][1],"[0-9]+(\\.[0-9]+)?"))
    
    balance_value<-as.numeric(str_extract(number_rupee[[1]][2],"[0-9]+(\\.[0-9]+)?"))
    
    #################################################################################
    
    ###############debited /credited messages .
  
    message_transaction_type<-0
     
    if(grepl("debit",tolower(all_transatsplit_frame$body[x]))|grepl("debited",tolower(all_transatsplit_frame$body[x]))){
      
      message_transaction_type<-"debit_message"
    }else if(grepl("credit",tolower(all_transatsplit_frame$body[x]))|grepl("credited",tolower(all_transatsplit_frame$body[x])))
    {
      message_transaction_type<-"credit_message"
    }else{
      
      message_transaction_type<-"balance_message"
    }
    
    
    
    ###########deduction type .
    
    deduction_transaction_type<-0
    
    if(grepl("pos",tolower(all_transatsplit_frame$body[x]))){
      
      deduction_transaction_type<-"pos_message"
    }else if(grepl("transfer",tolower(all_transatsplit_frame$body[x])))
    {
      deduction_transaction_type<-"transfer_message"
    }else if(grepl("atm",tolower(all_transatsplit_frame$body[x]))){
      
      deduction_transaction_type<-"atm_message"
    }else{
      
      deduction_transaction_type<-"others_message"
    }
    
    ########################################
    
    ############bank  name .
    addresses<-all_transatsplit_frame$address[x]
    
    bank_name<-substr(addresses,4,15)
    
    ################################################
    
    
    
    sorting_account<- c("a/c","account","ac","card")
    
    accoun_card_number<-as.character(paste(split_trans[x,1]))   ########card number.
    put_address_account<-length(tolower(splitted_value_by_ragex_first)) ## getting length  of the sorted  address section containing  prom and trans msg.
    
    get_address_account<-length(sorting_account[1:3]) ### finding  length  of the sort keys .
    srt<-sorting_account[1:3]
    output_account<-matrix(ncol=get_address_account,nrow=put_address_account) ### matrix  to  find  out the  distance  betwn keywrds and adress section .
    
    # tryCatch({
    #   for(i in 1:put_address_account)  ### function to find out  the count  of  promotionals .
    #   {
    #     output_account[,i]<-str_count(tolower(splitted_value_by_ragex_first),srt[i])
    #    output_account[is.na(output_account)]<-0
    #   }
    #   
    # }, error=function(e){})
    # 
    
    output_account<- sum(matrix(str_count(tolower(splitted_value_by_ragex_first),srt)))
    
    #sum_account<-data.frame(rowSums(output_account))  ##  sum  up the counts  .
    #sum_account[is.na(sum_account)]<-0
    sum_account<-output_account
    
    
    get_address_accountcard<-length(sorting_account[4]) ### finding  length  of the sort keys .
    output_account_card<-matrix(ncol=get_address_account,nrow=put_address_account) ### matrix  to  find  out the  distance  betwn keywrds and adress section .
    
    output_account_card<- str_count(tolower(splitted_value_by_ragex_first),sorting_account[4])
     
    sum_account_card<-output_account_card  ##  sum  up the counts  .
    sum_account_card[is.na(sum_account)]<-0
    
    if(sum_account=="0" & sum_account_card =="1"){
      
      transaction_medium<-"card_type "
    }else if(sum_account=="1" | sum_account=="2"){
      transaction_medium<-"account_type"
    }else {
    
    splitted_value_by_ragex_second<-strsplit(as.character(all_transatsplit_frame$body[x]),split=as.character(split_trans[x,1]),fixed=TRUE)[[1]][2]
    
    strcount<-function(w){  ###counting the pattern distance .
      
    gr<-grep(paste("\\b",sorting_account[w],"\\b",sep=""),tolower(splitted_value_by_ragex_second))
    
    if(length(gr)==1){
      
      splitted_value_by_count<-strsplit(as.character(tolower(splitted_value_by_ragex_second[1])),split=as.character(sorting_account[w]),fixed=TRUE)
      split_var_count<-nchar(splitted_value_by_count[[1]][1])
   
     }else if(length(gr)==0) {
      
      split_var_count<-0
     }
    
    
    srt_var<-data.frame(sorting_account[w],split_var_count)
    
    return(srt_var)
    
    }
    
    
    dist_trans<-do.call(rbind.data.frame,lapply( seq( sorting_account),function(k){strcount(k) })) ###taking out the distance from string2 to all the keywords.
    dist_trans<-dist_trans[dist_trans$split_var_count!=0,] ####removing zero  to find distance ,as zero doesnt add.
    short_dist<- subset(dist_trans,dist_trans$split_var_count== min(dist_trans$split_var_count))
    colnames(short_dist)<-c("type","dist")
    
    if(short_dist$type=="a/c"|short_dist$type=="account" |short_dist$type=="ac"){
      transaction_medium<-"account_type"
    }else{
      transaction_medium<-"card_type"
    }
    }
    
  # return(splitted_value_by_ragex_first)
    
  

    ######################################################
    
    account_card_based_transaction_details<<-0
    
account_card_based_transaction_details<<-data.frame(as.character(paste(bank_name)),as.character(paste(transaction_medium)), as.character((accoun_card_number)),amount_value,balance_value, as.character(paste0(message_transaction_type)), as.character(paste0(deduction_transaction_type)))  

   colnames(account_card_based_transaction_details)<-c("bank_name" ,"transaction_medium" , "accoun_card_number" ,"amount_value" ,"balance_value" ,"message_transaction_type","deduction_transaction_type")
   
   
account_card_based_transaction_details<-data.frame(levels(account_card_based_transaction_details$bank_name[1]),levels(account_card_based_transaction_details$transaction_medium[1]), levels(account_card_based_transaction_details$accoun_card_number[1]),amount_value,balance_value, levels(account_card_based_transaction_details$message_transaction_type[1]),levels(account_card_based_transaction_details$deduction_transaction_type[1]))  
colnames(account_card_based_transaction_details)<-c("bank_name" ,"transaction_medium" , "accoun_card_number" ,"amount_value" ,"balance_value" ,"message_transaction_type","deduction_transaction_type")

   
      }, error=function(e){
        
        
account_card_based_transaction_details<<-data.frame(bank_name=0,transaction_medium=0, accoun_card_number=0,amount_value=0,balance_value=0, message_transaction_type=0, deduction_transaction_type=0)  
        
      })
    return(account_card_based_transaction_details)
    
    
    }
    
    #######################################################################
    
 
    ####looping  through  all  messages   
all_account_credit<-do.call(rbind.data.frame,lapply(seq(nrow( split_trans)),function(k){breaking_function(k)}))
    
row.names(all_account_credit)<-row.names(all_transatsplit_frame)

####merging with  original message dataframe .

merging_msg_details_original<-cbind(df[row.names(all_account_credit),],all_account_credit)
original_after_remove_duplicates<-merging_msg_details_original[,!duplicated(colnames(merging_msg_details_original), fromLast = TRUE)]
original_after_remove_duplicates$bank_name<-as.character(original_after_remove_duplicates$bank_name)
original_after_remove_duplicates$transaction_medium<-as.character(original_after_remove_duplicates$transaction_medium)
original_after_remove_duplicates$accoun_card_number<-as.character(original_after_remove_duplicates$accoun_card_number)
original_after_remove_duplicates$message_transaction_type<-as.character(original_after_remove_duplicates$message_transaction_type)
original_after_remove_duplicates$deduction_transaction_type<-as.character(original_after_remove_duplicates$deduction_transaction_type)


df[row.names(original_after_remove_duplicates),]<-original_after_remove_duplicates

#####################################################################################################
    
    
    
    
    
    
    
    k<-data.frame(df)
    c = mongo(collection =id[db], db = "sms_data") ## Creating/using db and to make a connection .
    c$insert(k)
    







####################################################################################################
########################Updation  function  ends  here ############################################
#####################################################################################################

#################################################################################################
##########################Looping  through S3  data's  to update mongo #########################
############################################################s##################################
lapply(seq(call_S3_character),function(k){tryCatch({ all_data_db(k)
  
  
},error=function(e){})
  })  ## looping through  sequence of datas .

#############################################################################################
#######################Looping  of all  sms data  looping  ends here ########################
################################################################################################# 
 
    