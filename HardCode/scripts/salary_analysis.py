# -*- coding: utf-8 -*-
"""salary_analysis

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1FCsqWI5Bd51mWhLNRUHz6M0fitCveGnd
"""

import pandas as pd
import regex as re
import numpy as np
import threading


def check_body_1(df, pattern):
    d = []
    for index, row in df.iterrows():
        try:
            matcher = re.search(pattern, row["body"].lower())
        except:
            print(row)
            # exit(1)
        if (matcher != None):
            d.append(index)
    return d


def check_body_2(df, pattern):
    # path='/home/credicxo/Downloads/Trans 17758/checking/'
    d = []
    # check=[]
    for index, row in df.iterrows():
        matcher = re.search(pattern, row["body"].lower())
        if (matcher != None):
            # check.append(row)
            d.append(index)
    '''if(len(check)>0):
        with open(path+pattern+'.txt', 'w') as f:
            for item in check:
                f.write("%s\n" % item)'''
    return d


def check_header(df, pattern):
    # path='/home/credicxo/Downloads/Trans 17758/checking/'
    d = []
    # check=[]
    for index, row in df.iterrows():
        if (pattern in row["sender"].lower()):
            d.append(index)
            # check.append(row)
    '''if len(check)>0:
        with open(path+pattern+'.txt', 'w') as f:
            for item in check:
                f.write("%s\n" % item)'''
    return d


def thread_for_cleaning_1(df, pattern, result):
    result.append(check_body_1(df, pattern))


def thread_for_cleaning_2(df, pattern, result):
    result.append(check_body_2(df, pattern))


def thread_for_cleaning_3(df, pattern, result):
    result.append(check_header(df, pattern))


def cleaning(df):
    try:
        df.columns = ['cust_id', 'sender', 'body', 'timestamp', 'read']
    except:
        df.columns = ['a', 'cust_id', 'sender', 'body', 'timestamp', 'read']
        df = df.drop(columns='a')

    transaction_patterns = ['debited', 'credited']
    thread_list = []
    results = []

    # print(df.head())
    # print(df.shape)
    length = set(range(df.shape[0]))

    for pattern in transaction_patterns:
        thread = threading.Thread(target=thread_for_cleaning_1, args=(df, pattern, results))
        thread_list.append(thread)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    for i in results:
        length = length - set(i)

    df = df.drop(list(length))
    # df.to_csv (loc2, index = None, header=True)
    df.reset_index(drop=True, inplace=True)

    cleaning_transaction_patterns = ['request received to', 'received a request to add', 'premium receipt',
                                     'contribution',
                                     'data benefit', 'team hr', 'free [0-9]* ?[gm]b', ' data ', 'voucher', 'data pack',
                                     'benefit of ', 'data setting', 'added/ ?modified a beneficiary',
                                     'added to your beneficiary list', 'after activation', 'new beneficiary',
                                     'refund credited', 'return request', 'received request',
                                     'documents have been received',
                                     'last day free', 'received a refund', 'will be processed shortly',
                                     'credited a free',
                                     'request for modifying', 'free \d* [gm]b/day', 'data pack',
                                     'request for registration',
                                     'received by our company', 'month of', 'received a call', 'free data',
                                     'data benefits',
                                     'received full benefit', 'payment against', 'auto debited', 'mandates',
                                     'We apologize for the incorrect SMS',
                                     'coupon', 'can be credited ', 'no hassle of adding beneficiary', 'you\'re covered',
                                     'bank will never ask you to', 'eAadhaar', 'great news!', 'your query has',
                                     'redemption request', 'number received',
                                     'your order', 'beneficiary [a-z]*? is added successfully', 'dear employee',
                                     'subscribing', 'sorry',
                                     'received \d*? enquiry', 'congratulations?', 'woohoo!', 'hurry',
                                     'sign up', 'credited to your wallet', 'safe & secure!', '[gm]b is credited on',
                                     'cash reward',
                                     'remaining emi installment', 'incentive amount ', 'dear investor',
                                     'verification code', 'outstanding dues', 'congrat(ulation)?s', 'available limit ',
                                     'oyo money credited',
                                     'reminder', 'card ?((holder)|(member))', 'login request', 'cashback',
                                     'electricity bill', 'data pack activation',
                                     'paytm postpaid bill', 'failed', 'declined', 'cardmember', 'credit ?card',
                                     ' porting ', 'lenskart',
                                     'activated for fund transfer', 'biocon', 'updated wallet balance', 'recharging',
                                     'assessment year', 'we wish to inform', 'refunded',
                                     'amendment', 'added/modified', 'kyc verification', 'is due', 'paytm postpaid',
                                     'please pay', 'flight booking', 'offer',
                                     '(credited)?(received)? [0-9]*[gm]b', 'payment.*failed',
                                     'uber india systems pvt ltd', 'has requested money', 'on approving',
                                     'not received', 'received your', 'brand factory has credited ', 'train ticket',
                                     'total (amt)?(amount)? due', 'redbus wallet', 'otp',
                                     'due of', 'received ?a? ?bill', 'successful payments', 'response ', 'last day',
                                     'payment confirmation', 'payment sms', 'kyc',
                                     'added beneficiary', 'received a message', ' premium ', 'claim', 'points ',
                                     'frequency monthly', 'received a pay rise', 'cheque book',
                                     'will be', 'unpaid', 'received (for|in) clearing', 'presented for clearing',
                                     'your application', 'to know', 'unpaid',
                                     'thanking you', 'redeem', 'transferred', 'available credit limit']

    garbage_rows = []
    thread_list = []
    results = []

    for pattern in cleaning_transaction_patterns:
        thread = threading.Thread(target=thread_for_cleaning_2, args=(df, pattern, results))
        thread_list.append(thread)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    for i in results:
        garbage_rows.extend(i)

    df = df.drop(list(set(garbage_rows)))
    df.reset_index(drop=True, inplace=True)
    cleaning_transaction_patterns_header = ['vfcare',
                                            'oyorms',
                                            'payzap',
                                            'rummy',
                                            'polbaz',
                                            '600010',
                                            'rummyc',
                                            'rupmax',
                                            'ftcash',
                                            'dishtv',
                                            'bigbzr',
                                            'olamny',
                                            'bigbkt',
                                            'olacab',
                                            'urclap',
                                            'ubclap',
                                            'qeedda',
                                            'myfynd',
                                            'gofynd',
                                            'paytm',
                                            'airbnk',
                                            'phonpe',
                                            'paysns',
                                            'fabhtl',
                                            'spcmak',
                                            'cuemth',
                                            'zestmn',
                                            'pcmcmh',
                                            'dlhvry',
                                            'bludrt',
                                            'airtel',
                                            'acttvi',
                                            'erecharge',
                                            'swiggy',
                                            'fpanda',
                                            'simpl',
                                            'mytsky',
                                            'vodafone',
                                            'sydost',
                                            'ipmall',
                                            'quikrr',
                                            'mytsky',
                                            'lenkrt',
                                            'flpkrt',
                                            'flasho',
                                            'grofrs',
                                            'hdfcsl',
                                            'idhani',
                                            'adapkr',
                                            'ipmall',
                                            'oxymny',
                                            'jionet',
                                            'kissht',
                                            '155400',  # m-pesa
                                            'kredtb',
                                            'shoekn',
                                            'lzypay',
                                            'mobikw',
                                            'notice',
                                            'payltr',
                                            'swiggy',
                                            'vishal',
                                            'qira',
                                            'domino',
                                            'dinout',
                                            'quikrd',
                                            'goibib',
                                            'cureft',
                                            'olacbs',
                                            'ryatri',
                                            'dhanip',
                                            'zestmo',
                                            'smart']
    garbage_header_rows = []
    thread_list = []
    results = []

    for pattern in cleaning_transaction_patterns_header:
        thread = threading.Thread(target=thread_for_cleaning_3, args=(df, pattern, results))
        thread_list.append(thread)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    for i in results:
        garbage_header_rows.extend(i)
    '''yes=[]
    for i in range(df.shape[0]):
        if i in list(set(garbage_rows)):
            yes.append(True)
        else:
            yes.append(False)
    pd.concat([df2,df[yes]])'''

    df = df.drop(list(set(garbage_header_rows)))
    df.reset_index(drop=True, inplace=True)

    g = []
    for index, row in df.iterrows():
        matcher_1 = re.search("[Rr]egards", row["SMS"])
        if (matcher_1 != None):
            if ('DHANCO' not in row["sender"]):
                g.append(index)

    df = df.drop(list(set(g)))
    df.reset_index(drop=True, inplace=True)
    # print(df.head())
    # df2.to_csv(loc2[:-4]+"_noise_deleted.csv")
    # df.to_csv (loc2, index = None, header=True)
    return df


import pandas as pd
import numpy as np
import regex as re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait, as_completed


def get_credit_amount(data):
    data['credit_amount'] = [0] * data.shape[0]
    pattern_2 = '(?i)credited.*?(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern_1 = '(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?).*?credited'
    # pattern_3 = "credited with salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern_4 = '(?i)(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?).*?deposited'
    pattern_5 = '(?i)(?:(?:rs|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?).*?received'
    pattern_6 = '(?i)received.*?(?:(?:rs|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    # pattern_7 = "salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*credited"
    # pattern_debit_1 = '(?i)debited(.*)?(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern_debit_2 = 'credited to beneficiary'

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        # matcher_3 = re.search(pattern_3,message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)

        amount = 0
        if matcher_1 != None:
            # matcher_debit_1 = re.search(pattern_debit_1,message)
            matcher_debit_2 = re.search(pattern_debit_2, message)
            if (matcher_debit_2 != None):
                amount = 0

            else:
                amount = matcher_1.group(1)

        elif matcher_2 != None:
            amount = matcher_2.group(1)


        elif matcher_4 != None:
            amount = matcher_4.group(1)
        elif matcher_5 != None:
            amount = matcher_5.group(1)
        elif matcher_6 != None:
            amount = matcher_6.group(1)


        else:
            amount = 0
        try:
            data['credit_amount'][i] = float(str(amount).replace(",", ""))
        except Exception as e:
            print(e)
            print(i + 2)
            print(str(amount).replace(",", ""))
    return data


def divide_and_sort(loc1, loc2, name):
    dfa = pd.read_csv(loc1)
    for group_name, df in dfa.groupby(name):
        with open(loc2 + str(group_name) + '.csv', 'w') as f:
            df.to_csv(f, index=None, header=True)


def get_epf_amount(data):
    data["epf_amount"] = [0] * data.shape[0]
    pattern1 = "(?:[Ee][Pp][Ff] [Cc]ontribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = "(?:passbook balance).*?(?:contribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"

    for i, row in data.iterrows():
        m = row["body"].lower()

        y1 = re.search(pattern1, m)
        y2 = re.search(pattern2, m)
        if (y1 != None):
            amount = y1.group(3)
        elif (y2 != None):
            amount = y2.group(3)
        else:
            amount = 0
        data["epf_amount"][i] = float(str(amount).replace(",", ""))
    return data


def epf_to_salary(data, column):
    data["salary"] = [0] * data.shape[0]
    for i in range(0, data.shape[0]):
        data["salary"][i] = (data[column][i] * 100) / 12
    return data


def get_salary(data):
    data["direct_sal"] = [0] * data.shape[0]
    pattern1 = "credited with salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = "salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*credited"

    for i, row in data.iterrows():
        m = row["body"].lower()

        y1 = re.search(pattern1, m)
        y2 = re.search(pattern2, m)
        if (y1 != None):
            amount = y1.group(3)
        elif (y2 != None):
            amount = y2.group(3)
        else:
            amount = 0
        data["direct_sal"][i] = float(str(amount).replace(",", ""))
    return data


def get_time(data):
    for i in range(data.shape[0]):
        x = datetime.strptime(data['timestamp'].values[i], "%Y-%m-%d %H:%M:%S")
        data['timestamp'].values[i] = x
    return data


def salary_check(file):
    import json
    ## READING JSON FILE AND CHANGING IN DATAFRAME
    with open(file, 'r') as f:
        d = json.load(f)

    name = f.name.split('/')[-1].split('.')[0]  # GETTING cust_id TO ADD AS COLUMN
    d = pd.DataFrame(d)
    d = d.transpose()
    d['timestamp'] = d.index
    d['cust_id'] = d.index
    for i in range(d.shape[0]):
        d['timestamp'][i] = datetime.utcfromtimestamp(int(d.index[i]) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        d['cust_id'][i] = name  # Appending  cust_id as column in dataframe
    d.reset_index(inplace=True)
    d = d.drop('index', axis=1)
    d = d.sort_index(axis=1)
    columns_titles = ['cust_id', 'timestamp', 'body', 'sender', 'read']
    d = d.reindex(columns=columns_titles)
    # data=pd.read_csv(file)
    # print(data)
    data = cleaning(d)
    grouper = pd.Grouper(key='timestamp', freq='M')
    data = get_time(data)
    var1 = True
    var2 = True
    salary = 0

    data = get_epf_amount(data)
    data = epf_to_salary(data, "epf_amount")
    df_salary = data.groupby(grouper)['salary'].max()

    if (df_salary[-1] != 0):
        salary = df_salary[-1]
        var1 = False

    if var1:
        data = get_salary(data)
        df_d_salary = data.groupby(grouper)['direct_sal'].max()
        if (df_d_salary[-1] != 0):
            salary = df_salary[-1]
            var2 = False

    if var2:
        data = get_credit_amount(data)

        data["credit_amount"] = np.where(data["credit_amount"] >= 10000, data["credit_amount"], 0)

        df_credit = data.groupby(grouper)['credit_amount'].max()

        df_final_sal = pd.DataFrame(df_credit.tail())

        print(df_final_sal)

        if df_final_sal.shape[0] > 1:
            if ((df_final_sal["credit_amount"][-1] != 0) and (df_final_sal["credit_amount"][-2] != 0)):

                real_money = list(df_final_sal['credit_amount'])[::-1]
                month = [w.month for w in list(df_final_sal.index)][::-1]
                a1 = True
                a2 = False
                # a3=False
                list_date = []
                for i in range(data.shape[0]):

                    if a1:
                        if data['credit_amount'][i] == real_money[0]:
                            list_date.append(data['timestamp'][i])
                            a1 = False
                            a2 = True
                    elif a2:
                        if data['credit_amount'][i] == real_money[1]:
                            if data['timestamp'][i].month == month[1]:
                                list_date.append(data['timestamp'][i])
                                a2 = False
                                a3 = True
                                break
                    '''elif a3:
            if data['credit_amount'][i]==real_money[2]:
              if data['TIMESTAMP'][i].month == month[2]:    
                list_date.append(data['TIMESTAMP'][i])
                break '''

                    from datetime import timedelta

                time1 = list_date[0] + timedelta(days=34)
                time2 = list_date[0] - timedelta(days=34)

                val1 = df_final_sal["credit_amount"][-1] + df_final_sal["credit_amount"][-1] / 4
                val2 = df_final_sal["credit_amount"][-1] - df_final_sal["credit_amount"][-1] / 4
                if (time2 < list_date[1] < time1):
                    if (val2 < df_final_sal["credit_amount"][-2] < val1):
                        salary = (df_final_sal["credit_amount"][-1] + df_final_sal["credit_amount"][-2]) / 2
                    else:
                        print("no salary found")
                        return

    return salary
