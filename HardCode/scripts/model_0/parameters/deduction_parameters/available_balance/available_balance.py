#from djangoscripts.settings import BASE_DIR
from datetime import datetime
from pytz import timezone
import pandas as pd
import csv
from pprint import pprint
from pymongo import MongoClient


def conn():
    # Create MONGO_SUPERUSER and MONGO_SUPERPASS global varaible in local environment for MongoDB

    connection = MongoClient(
        f"mongodb://root:root123@localhost:27017/?authSource=admin"
                                       f"&readPreference=primary&ssl=false"
,
        socketTimeoutMS=900000)
    return connection

def month_balance(value,prev_month,prev_year,sec_mon,sec_yr,third_mon,third_yr,all_timestamps,sms_info_df,ac_no,tz_info):
    last_month_bal = int(value['last_month_available_balance'])
    second_last_month_bal = value['second_last_month_bal']
    index_second_last = value['index_second_last_month'] + 1
    third_last_month_bal = value['third_last_month_bal']
    index_third_last = value['index_third_last_month'] + 1
    index_last_month = value['index_last_month'] + 1
    temp = True
    index = index_third_last
    while temp and index < len(all_timestamps):
        ac_no_sheet = str(sms_info_df['acc_no'][index])
        if len(ac_no_sheet) > 3:
            ac_no_sheet = ac_no_sheet[-3:]
        if ac_no_sheet != ac_no:
            index += 1
            continue
        timestamp_temp = datetime.strptime(all_timestamps[index], '%Y-%m-%d %H:%M:%S')
        timestamp_temp = timestamp_temp.replace(tzinfo=tz_info)
        credit_amnt = int(sms_info_df['Credit Amount'][index])
        debit_amnt = int(sms_info_df['Debit Amount'][index])
        if (timestamp_temp.month <= third_mon):
            if (timestamp_temp.year <= third_yr):
                if credit_amnt != 0:
                    third_last_month_bal += credit_amnt
                if debit_amnt != 0:
                    third_last_month_bal = third_last_month_bal - debit_amnt

        elif (timestamp_temp.year < third_yr):
                if credit_amnt != 0:
                    third_last_month_bal += credit_amnt
                if debit_amnt != 0:
                    third_last_month_bal = third_last_month_bal - debit_amnt
        if index >= index_second_last:
            if (timestamp_temp.month <= sec_mon):
                if (timestamp_temp.year <= sec_yr):
                    if credit_amnt != 0:
                        second_last_month_bal += credit_amnt
                    if debit_amnt != 0:
                        second_last_month_bal = second_last_month_bal - debit_amnt
            elif (timestamp_temp.year < sec_yr):
                if credit_amnt != 0:
                    second_last_month_bal += credit_amnt
                if debit_amnt != 0:
                    second_last_month_bal = second_last_month_bal - debit_amnt


        if index >= index_last_month:
            if(timestamp_temp.month <= prev_month):
                if (timestamp_temp.year <= prev_year):
                    if credit_amnt != 0:
                        last_month_bal += credit_amnt
                    if debit_amnt != 0:
                        last_month_bal = last_month_bal - debit_amnt
            elif (timestamp_temp.year < prev_year):
                if credit_amnt != 0:
                    last_month_bal += credit_amnt
                if debit_amnt != 0:
                    last_month_bal = last_month_bal - debit_amnt

        elif timestamp_temp.month > prev_month:
            if (timestamp_temp.year >= prev_year):
                temp = False
        elif (timestamp_temp.year > prev_year):
            temp = False
        index += 1
    return [last_month_bal,second_last_month_bal,third_last_month_bal]

def find_info(loan_date_time,id):
    # user_id = 364513
    # latest_loan = Loans.objects.filter(user__id = user_id).order_by('loan_date').reverse()[0]
    # loan_date_time = datetime.strptime('26-03-2020 07:12:22','%d-%m-%Y %H:%M:%S')
    connect = conn()
    user_data = connect.analysis.balance_sheet.find_one({'cust_id':id})
    user_data = user_data['sheet']

    sms_info_df = pd.DataFrame(user_data)
    sms_info_df.to_csv("sms_availble_bal"+str(id)+".csv")

    all_timestamps = list(sms_info_df['timestamp'])
    unique_acc_dict = {}
    # loan_date_time = latest_loan.loan_date
    loan_date_time.astimezone(timezone("Asia/kolkata"))
    tz_info = loan_date_time.tzinfo
    if loan_date_time.month == 1:
        previous_month = 12
        previous_year = (loan_date_time.year - 1)
    else:
        previous_month = (loan_date_time.date().month - 1)
        previous_year = loan_date_time.date().year
    if previous_month == 1:
        second_last_month = 12
        second_last_year = previous_year - 1
    else:
        second_last_month = previous_month -1
        second_last_year = previous_year
    if second_last_month == 1:
        third_last_month = 12
        third_last_year = second_last_year - 1
    else:
        third_last_month = second_last_month -1
        third_last_year = second_last_year
    for i in range(len(all_timestamps)-1,0,-1):
        timestamp = datetime.strptime(all_timestamps[i],'%Y-%m-%d %H:%M:%S')
        timestamp = timestamp.replace(tzinfo=tz_info)
        last_bal = 0
        last_second_bal = 0
        last_third_bal = 0
        if timestamp.month <= previous_month:
            if timestamp.year <= previous_year:


                last_bal = int(sms_info_df['Available Balance'][i])
        elif timestamp.year < previous_year:
            last_bal = int(sms_info_df['Available Balance'][i])

        if timestamp.month <= second_last_month:
            if timestamp.year <= second_last_year:
                last_second_bal = int(sms_info_df['Available Balance'][i])
        elif timestamp.year < second_last_year:
            last_second_bal = int(sms_info_df['Available Balance'][i])

        if timestamp.month <= third_last_month:
            if timestamp.year <= third_last_year:
                last_third_bal = int(sms_info_df['Available Balance'][i])
        elif timestamp.year < third_last_year:
            last_third_bal = int(sms_info_df['Available Balance'][i])

        ac_no = str(sms_info_df['acc_no'][i])
        if len(ac_no) > 3:
            ac_no = ac_no[-3:]


        try:

            x = unique_acc_dict[ac_no]
            if x['Available_balance'] == 0:
                x['Available_balance'] = int(sms_info_df['Available Balance'][i])
                x['index_latest_timestamp'] = i
            if x['last_month_available_balance'] == 0:
                x['last_month_available_balance'] = last_bal
                x['index_last_month'] = i

            if x['second_last_month_bal'] == 0:#and timestamp.month <= second_last_month and timestamp.year <= second_last_year:
                x['second_last_month_bal'] = last_second_bal
                x['index_second_last_month'] = i

            if x['third_last_month_bal'] == 0:  #and timestamp.month <= third_last_month and timestamp.year <= third_last_year:
                x['third_last_month_bal'] = last_third_bal
                x['index_third_last_month'] = i
            # if x['Available_balance'] != 0 and x['last_month_available_balance'] != 0:

        except Exception as e:
            # print(all_timestamps[i]

            unique_acc_dict.update({ac_no:
                                        {'index_latest_timestamp':i,
                                         'index_last_month':i,
                                         'Available_balance': int(sms_info_df['Available Balance'][i]),
                                         'last_month_available_balance':last_bal,
                                         'index_second_last_month':i,
                                         'second_last_month_bal':last_second_bal,
                                         'index_third_last_month': i,
                                         'third_last_month_bal': last_third_bal
                                         }})

            # x = unique_acc_dict[sms_info_df['acc_no']]
            continue

    list_to_return = []
    for ac_no,value in unique_acc_dict.items():
        csv_dict = {}
        csv_dict['AC_NO'] = ac_no
        latest_avail_bal = int(value['Available_balance'])
        index_latest_timestamp = value['index_latest_timestamp'] + 1
        while index_latest_timestamp < len(all_timestamps):
            ac_no_sheet = str(sms_info_df['acc_no'][index_latest_timestamp])
            if len(ac_no_sheet) > 3:
                ac_no_sheet = ac_no_sheet[-3:]
            if ac_no_sheet != ac_no:
                index_latest_timestamp += 1
                continue
            if sms_info_df['Credit Amount'][index_latest_timestamp] != 0:
                latest_avail_bal += int(sms_info_df['Credit Amount'][index_latest_timestamp])

            if sms_info_df['Debit Amount'][index_latest_timestamp] != 0:
                latest_avail_bal = latest_avail_bal - int(sms_info_df['Debit Amount'][index_latest_timestamp])
            index_latest_timestamp += 1

        bal_list = month_balance(value,
                                previous_month,
                                previous_year,
                                second_last_month,
                                second_last_year,
                                third_last_month,
                                third_last_year,
                                all_timestamps,
                                sms_info_df,
                                ac_no,
                                tz_info)


        csv_dict.update({'balanace_on_loan_date':latest_avail_bal,'last_month_bal':bal_list[0],
                         'second_last_month_bal':bal_list[1],
                         'third_last_month_bal':bal_list[2]})
        list_to_return.append(csv_dict)


    return list_to_return

        # wr.writerow(csv_dict)



loan_date_time = datetime.now()
list_user = [355628, 360092, 356935, 359537]
for user in list_user:
    pprint(find_info(loan_date_time,user))


