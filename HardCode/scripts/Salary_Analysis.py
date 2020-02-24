import pandas as pd
import numpy as np
import regex as re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from datetime import timedelta
import pymongo
import json
import pprint
from pymongo import MongoClient
import sys
from tqdm import tqdm
from Util import logger_1


def clean_debit(data):
    '''This code drops the rows for debited messages and bhanix finance company messages.

        Parameters: DataFrame.

        Output: DataFrame.

        '''
    logger=logger_1("Clean Debit",id)
    logger.info("Cleaning text data")
                    
    
    pattern1 = "bhanix"
    pattern2 = "debited"
    d = []
    for i, row in data.iterrows():
        message = row["body"].lower()
        y1 = re.search(pattern1, message)
        y2 = re.search(pattern2, message)

        if y1 != None or y2 != None:
            d.append(i)

    data.drop(d, inplace=True)
    data.reset_index(drop=True, inplace=True)

    return data


def get_credit_amount(data):
    '''
    This code finds the credited amount from the messages in a DataFrame.

          Parameters: DataFrame.

          Output: DataFrame.

          '''
    logger=logger_1("Get Credit Data",id)
    logger.info("Credit Amount")
    
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


def get_epf_amount(data):
    '''This code finds the epf(employee provident fund) amount from the messages in the DataFrame.

          Parameters: DataFrame.

          Output: DataFrame.

          '''
    
    logger=logger_1("Get Epf Amount",id)
    logger.info("Epf Amount")

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
    '''This code calculates the salary from the epf amount with formula: epf=12% of salary.

          Parameters: DataFrame.

          Output: DataFrame.

          '''
    logger=logger_1("Epf Salary",id)
    logger.info("Epf Salary Amount")

    data["salary"] = [0] * data.shape[0]
    for i in range(0, data.shape[0]):
        data["salary"][i] = (data[column][i] * 100) / 12
    return data


def get_salary(data):
    '''This code finds the salary from the messages if keyword 'salary' is found.

          Parameters: DataFrame.

          Output: DataFrame.

          '''
    
    logger=logger_1('Get Salary',id)
    logger.info('Direct Salary Amount')
  
    data["direct_sal"] = [0] * data.shape[0]
    pattern1 = "credited with salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = "salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*credited"
    pattern3 = "(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?imps\/salary"

    for i, row in data.iterrows():
        m = row["body"].lower()

        y1 = re.search(pattern1, m)
        y2 = re.search(pattern2, m)
        y3 = re.search(pattern3, m)
        if (y1 != None):
            amount = y1.group(3)
        elif (y2 != None):
            amount = y2.group(3)
        elif (y3 != None):
            amount = y3.group(3)
        else:
            amount = 0
        data["direct_sal"][i] = float(str(amount).replace(",", ""))
    return data


def get_time(data):
    '''This code converts the timestamp from unix format to datetime.

          Parameters: DataFrame.

          Output: DataFrame.

          '''
    
    logger=logger_1("Get Time",id)
    logger.info("Convert Timestamp To Datetime")
    
    for i in range(data.shape[0]):
        try:
            x = datetime.strptime(data['timestamp'].values[i], "%Y-%m-%d %H:%M:%S")
            data['timestamp'].values[i] = x
        except:
            print("timestamp could not be converted at " + i)

    return data


def salary_check(data):
    '''This code calls all the function to calculate salary of a user based on the messages in dataFrame.

          Parameters: DataFrame.

          Output: DataFrame.

    '''
    logger=logger_1('Salary Check',id)
    logger.info('Salary Calculation Started')

    data = clean_debit(data)
    grouper = pd.Grouper(key='timestamp', freq='M')
    data = get_time(data)
    var1 = True
    var2 = True
    salary = 0
    keyword=""
   

    data = get_epf_amount(data)
    data = epf_to_salary(data, "epf_amount")
    df_salary = data.groupby(grouper)['salary'].max()
    

    try:
        logger.info('Calculating salary form EPF keyword')
        if (df_salary[-1] != 0):
            salary = df_salary[-1]
            keyword="EPF"

        elif (df_salary[-2] != 0):
            salary = df_salary[-2]
            keyword="EPF"
            
            var1 = False
            var2 = False
    except:
        salary = None

        if var1:
            try:
                logger.info('Calculating salary form Salary keyword')
                data = get_salary(data)
                df_d_salary = data.groupby(grouper)['direct_sal'].max()
                if (df_d_salary[-1] != 0):
                    salary = df_d_salary[-1]
                    keyword="Salary"
                elif (df_d_salary[-2] != 0):
                    salary = df_d_salary[-2]
                    keyword="Salary"
                    var2 = False
            except:
                salary = None

        if var2:
            try:
                logger.info('Calculating salary from credit messages')
                data = get_credit_amount(data)

                data["credit_amount"] = np.where(data["credit_amount"] >= 10000, data["credit_amount"], 0)

                df_credit = data.groupby(grouper)['credit_amount'].max()

                df_final_sal = pd.DataFrame(df_credit.tail())

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
                            if a2:
                                if data['credit_amount'][i] == real_money[1]:
                                    # if data['timestamp'][i].month == month[1]:
                                    list_date.append(data['timestamp'][i])
                                    # a2=False
                                    # a3=True
                                    break

                        # print(list_date)
                        time1 = list_date[0] - timedelta(days=26)
                        time2 = list_date[0] - timedelta(days=34)
                        val1 = df_final_sal["credit_amount"][-1] + df_final_sal["credit_amount"][-1] / 4
                        val2 = df_final_sal["credit_amount"][-1] - df_final_sal["credit_amount"][-1] / 4

                        if (time2 < list_date[1] < time1):
                            if (val2 < df_final_sal["credit_amount"][-2] < val1):
                                salary = (df_final_sal["credit_amount"][-1] + df_final_sal["credit_amount"][-2]) / 2
                                keyword="Credit"
                            else:

                                return
            except:
                salary = None
                logger.critical('salary not found')

    return salary,keyword


def conn():
    logger=logger_1('Connection',id)
    logger.info('Building connection')

    ''' This function create connection with mongodb database
    Parameters:
      Output: Returns connection object
     '''
    
    connection = MongoClient(
        "mongodb://god:rock0004@13.67.79.22:27017/?authSource=admin&readPreference=primary&ssl=false", maxPoolSize=200)
    return connection


def transaction(id):
    logger=logger_1('Transaction Data',id)
    logger.info('Collecting SMS from Transaction Collection')

    ''' This function connects with collection in mongodb database
    Parameters:
      Input : Customer Id
      Output: Dataframe
     '''
    
    connect = conn()
    transaction = connect.messagecluster.transaction
    file1 = transaction.find_one({"_id": id})
    x = pd.DataFrame(file1["sms"])

    return x


def extra(id):
    logger=logger_1('Extra Data',id)
    logger.info('Collecting SMS from Extra Collection')
    ''' This function find rows having epf as keyword in data
    Parameters :
      Input  :  Customer id
      Output :  Returns epf amount
    '''
    
    connect = conn()
    extra = connect.messagecluster.extra
    file2 = extra.find_one({"_id": id})
    y = pd.DataFrame(file2["sms"])

    epf = []
    for i in range(y.shape[0]):
        if re.search("EPFOHO", y["sender"][i]):
            epf.append(y.values[i])
    epf = pd.DataFrame(epf, columns=['sender', 'body', 'timestamp', 'read'])
    return epf


def merge(id):
    logger=logger_1('Merge Data',id)
    logger.info('Merging the Transaction and Extra SMS')

    ''' This code 
    Parameters:
     Input : Customer id
     Output: Dataframe
    ''' 
    
    tran = transaction(id)
    ext = extra(id)
    
    total = pd.concat([tran, ext], 0)
    total = total.reset_index(drop=True)
    return total


def customer_salary(id):
    '''This code first merges the data from the transaction and extra colection in mongodb

       then it calls the main function salary_check for calculating salary, .

          Parameters:
          id(int): id of the user.

          Output: Dictionary(user_id,status,message,salary)
          user_id(int)             : id of the customer
          status(bool)             : if the code runs successfully or not
          message(string)          : success/error/no salary found
          salary(int/  user_id=id Nonetype)     : salary of the customer found then int otherwise Nonetype.

    '''

    logger=logger_1('Customer Salary',id)
    logger.info('Checking salary status')

    try:
        salary_status = {}
        print(0)
        merged = merge(id)
        print(1)
        
        salary,keyword = salary_check(merged)
        print(2)
        salary_status["SALARY"] = salary

        if (salary == 0) | (salary == None):
            salary_status["SALARY"] = None
            status = False
            message = "No Salary Found"

        else:
            status = True
            message = "SUCCESS"
    except Exception as e:
        logger.critical('Error in code')
        status = False
        message = "ERROR"
        salary_status["SALARY"] = None
        

    salary_status["USER_ID"] = id
    salary_status["STATUS"] = status
    salary_status["MESSAGE"] = message
    salary_status["KEYWORD"] = keyword

    return salary_status




# main functions used to push data to mongodb
# def convert_json(data, name):
#     ''' This code used to push data to mongodb
#     Parameters :
#       Input : 
#          data : Dataframe
#          name : Customer Id
#       Output : Json object 
#       '''
#     logger=logger_1('Convert Json',id)
#     logger.info('Converting to Json file')
#     obj = {"SALARY": []}
#     for i in range(data.shape[0]):
#         salary = {"SALARY": int(data['SALARY'][i]),"KEYWORD":data["KEYWORD"][i]}
#         obj["SALARY"].append(salary)
#     return obj


def salary_analysis(id):
    logger=logger_1('Salary Analysis',id)
    logger.info('Salary Analysis started')
    
    ''' This function  call function to push salary in mongodb database
    Parameters  :  
       Input  : Customer id
       Output : Salary updated in mongodb database
    '''

    salary_dict = customer_salary(id)
    # print(salary_dict)
    # sal_df = pd.DataFrame(salary_dict, index=[0])
    # json_sal = convert_json(sal_df, id)
    json_sal={"_id":int(id),"Salary":int(salary_dict['SALARY']),"Keyword":salary_dict['KEYWORD']}
    key = {"_id": id}
    connect = conn()

    db = connect.analysis.salary
    db.update(key, json_sal, upsert=True)
    connect.close()


