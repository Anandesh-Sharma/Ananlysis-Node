import pandas as pd
# import numpy as np
import regex as re
from datetime import datetime, timedelta
import pytz
# from datetime import timedelta
# import json
from pymongo import MongoClient
from .Util import logger_1,conn

def clean_debit(data, id):
    """This code drops the rows for debited messages and bhanix finance company messages.

        Parameters: DataFrame.

        Output: DataFrame.

    """
    logger = logger_1("Clean Debit", id)
    # logger.info("Cleaning text data")

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
    # logger.info("Cleaning completed")
    return data


def get_epf_amount(data, id):
    """This code finds the epf(employee provident fund) amount from the messages in the DataFrame.

          Parameters: DataFrame.

          Output: DataFrame.

    """

    logger = logger_1("Get Epf Amount", id)
    # logger.info("Epf Amount Calculation starts")

    data["epf_amount"] = [0] * data.shape[0]
    pattern1 = r"(?:[Ee][Pp][Ff] [Cc]ontribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = r"(?:passbook balance).*?(?:contribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"

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
    # logger.info("epf amount calculation completed")
    return data


def epf_to_salary(data, column, id):
    """This code calculates the salary from the epf amount with formula: epf=12% of salary.

          Parameters: DataFrame.

          Output: DataFrame.

    """
    logger = logger_1("Epf to Salary", id)
    # logger.info("Salary Calculation from EPF Amount starts")

    data["salary"] = [0] * data.shape[0]
    for i in range(0, data.shape[0]):
        data["salary"][i] = (data[column][i] * 100) / 12
    # logger.info("Salary Calculation from EPF Amount complete")
    return data


def get_salary(data, id):
    """This code finds the salary from the messages if keyword 'salary' is found.

          Parameters: DataFrame.

          Output: DataFrame.

    """

    logger = logger_1('Get Salary', id)
    # logger.info('Direct Salary Amount Calculation starts')

    data["direct_sal"] = [0] * data.shape[0]
    pattern1 = r"credited with salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = r"salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*credited"
    pattern3 = r"(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?imps\/salary"

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
    # logger.info('Direct salary calculation completes')
    return data


def get_time(data, id):
    """
        This code converts the timestamp from unix format to datetime.

          Parameters: DataFrame.

          Output: DataFrame.

    """

    logger = logger_1("Get Time", id)
    # logger.info("Convert Timestamp To Datetime")

    for i in range(data.shape[0]):
        try:
            x = datetime.strptime(data['timestamp'].values[i], "%Y-%m-%d %H:%M:%S")
            data['timestamp'].values[i] = x
        except:
            logger.error("timestamp not converted")
            return {"status": False, "message": "timestamp not converted"}

    return {"status": True, "message": "success", 'data': data}


def salary_check(data, id):
    """This code calls all the function to calculate salary of a user based on the messages in dataFrame.

          Parameters: DataFrame.

          Output: Salary(int),Keyword(string).
    """

    logger = logger_1('Salary Check', id)
    # logger.info('Salary Calculation Started')

    data = clean_debit(data, id)
    if data.shape[0] == 0:
        return {'status': False, 'message': 'no messages found'}
    grouper = pd.Grouper(key='timestamp', freq='M')
    result = get_time(data, id)
    if not result['status']:
        return result
    data = result['data']
    var1 = True
    salary = 0
    keyword = ""

    data = get_epf_amount(data, id)
    data = epf_to_salary(data, "epf_amount", id)
    if data.shape[0] == 0:
        return {'status': False, 'message': 'no messages found'}
    df_salary = data.groupby(grouper)['salary'].max()

    # logger.info('Finding salary from EPF keyword')
    if len(df_salary) < 2:
        if df_salary[-1] != 0:
            salary = df_salary[-1]
            keyword = "EPF"
            var1 = False
        # logger.info("found salary from EPF keyword")

    else:
        if df_salary[-1] != 0:
            salary = df_salary[-1]
            keyword = "EPF"
            var1 = False
        # logger.info("found salary from EPF keyword")

        elif df_salary[-2] != 0:
            salary = df_salary[-2]
            keyword = "EPF"
            var1 = False
            # logger.info("found salary from EPF keyword")

    if var1:
        try:
            # logger.info('Finding salary from Salary keyword')
            data = get_salary(data, id)
            df_d_salary = data.groupby(grouper)['direct_sal'].max()
            if len(df_d_salary) < 2:

                if df_d_salary[-1] != 0:
                    salary = df_d_salary[-1]
                    keyword = "Salary"

            else:
                if df_d_salary[-1] != 0:
                    salary = df_d_salary[-1]
                    keyword = "Salary"
                # logger.info("salary found from salary keyword")
                elif df_d_salary[-2] != 0:
                    salary = df_d_salary[-2]
                    keyword = "Salary"
                # logger.info("salary found from salary keyword")

        except:
            salary = None
    return {'status': True, 'message': 'success', "salary": salary, "keyword": keyword}


def transaction(id):
    ''' This function connects with collection in mongodb database
      Parameters:
      Input : Customer Id
      Output: Dictionary with Parameters:    status(bool):code run successfully or not , 
                                                message(string):success/error ,  
                                                df(dataframe): dataframe of transaction data
      
    '''

    logger = logger_1('Transaction Data', id)
    # logger.info('Collecting SMS from Transaction Collection')

    connect = conn()
    transaction = connect.messagecluster.transaction

    file1 = transaction.find_one({"cust_id": id})
    if file1 is None:
        # logger.info("Transaction data not available")
        return {'status': False, 'message': "file doesn't exist"}
    x = pd.DataFrame(file1["sms"])

    return {'status': True, 'message': "success", "df": x}


def extra(id):
    """ This function find rows having epf as keyword in data
      Parameters :
      Input  :  Customer id(int)
      Output :  Returns epf messages dataframe
    """
    logger = logger_1('Extra Data', id)
    # logger.info('Collecting SMS from Extra Collection')

    connect = conn()
    extra = connect.messagecluster.extra
    file2 = extra.find_one({"cust_id": id})
    y = pd.DataFrame(file2["sms"])

    epf = []
    for i in range(y.shape[0]):
        if re.search("EPFOHO", y["sender"][i]):
            epf.append(y.values[i])
    epf = pd.DataFrame(epf, columns=['sender', 'body', 'timestamp', 'read'])
    return epf


def merge(id):
    """ This code
     Parameters:
     Input : Customer id(int)
     Output: Dictionary with Parameters:    status(bool):code run successfully or not ,
                                                message(string):success/error ,
                                                df(dataframe): dataframe of merged data
    """
    logger = logger_1('Merge Data', id)
    # logger.info('Merging the Transaction and Extra SMS')

    result = transaction(id)
    if not result['status']:
        return result
    tran = result['df']
    if tran.shape[0] != 0:
        logger.info("Data fetched from Transaction collection")
    else:
        logger.error("No data fetched from Transaction collection")
        return {'status': False, 'message': 'no transaction message', "salary": "0"}
    ext = extra(id)
    if ext.shape[0] != 0:
        logger.info("Data fetched from Extra collection")
    else:

        logger.info("No data fetched from Extra collection")

    total = pd.concat([tran, ext], 0)
    total = total.reset_index(drop=True)

    return {'status': True, 'message': 'success', 'total': total}


def customer_salary(id):
    """This code first merges the data from the transaction and extra colection in mongodb

       then it calls the main function salary_check for calculating salary, .

          Parameters:
          id(int): id of the user.

          Output: Dictionary(user_id,status,message,salary,keyword)
          user_id(int)             : id of the customer
          status(bool)             : if the code runs successfully or not
          message(string)          : success/error/no salary found
          salary(int/Nonetype)     : salary of the customer found then int otherwise Nonetype
          keyword(string)          : EPF/Salary/Credit.

    """

    logger = logger_1('Customer Salary', id)
    # logger.info('Checking salary status')
    salary_status = {}

    try:

        result = merge(id)
        if not result['status']:
            return result
        merged = result['total']
        if merged.shape[0] == 0:
            logger.error("Data not merged")
        else:

            logger.info("Data merged successfully")

        result = salary_check(merged, id)
        if not result['status']:
            return result
        salary, keyword = result['salary'], result['keyword']

        salary_status["salary"] = salary
        if (salary == 0) | (salary == None):
            salary_status["salary"] = "0"
            status = True
            message = "Salary Not found"
            # logger.info("not found salary")


        else:
            status = True
            message = "SUCCESS"
            # logger.info("salary calculated")
    except Exception as e:
        # logger.critical("salary not found")
        # logger.exception(e)
        status = False
        message = "ERROR"
        salary_status["salary"] = "0"
        keyword = None

    salary_status["cust_id"] = id
    salary_status["status"] = status
    salary_status["message"] = message
    salary_status["keyword"] = keyword
    return salary_status


def salary_analysis(id):
    """ This function  call function to push salary in mongodb database
       Parameters  :
       Input  : Customer id(int)
       Output : Dictionary with Parameters:     _id(int): user_id
                                                status(bool):code run successfully or not ,
                                                message(string):success/error/not found ,
                                                keyword(string): EPF/Salary
                                                salary(str): salary of user
    """
    logger = logger_1('Salary Analysis', id)
    # logger.info('Salary Analysis started')

    salary_dict = customer_salary(id)

    if salary_dict['status'] == False:
        connect = conn(id)
        key = {"cust_id": id}
        salary_dict['cust_id']=id
        salary_dict['modified_at']= str(datetime.now(pytz.timezone('Asia/Kolkata')))
        db = connect.analysis.salary
        db.update(key, salary_dict, upsert=True)
        # logger.info("salary updated in database")
        connect.close()
        return {'status': True, 'message': 'success', 'salary': 0, 'keyword': ""}
    else:

        json_sal = {"cust_id": int(id), "salary": str(salary_dict['salary']), "keyword": salary_dict['keyword']}
        salary_dict = {"cust_id": int(id), "salary": str(salary_dict['salary']), "keyword": salary_dict['keyword'],
                       'status': True, 'message': salary_dict["message"]}
        key = {"cust_id": id}
        connect = conn()

        db = connect.analysis.salary
        json_sal['modified_at']= str(datetime.now(pytz.timezone('Asia/Kolkata')))
        db.update(key, json_sal, upsert=True)
        # logger.info("salary updated in database")
        connect.close()

    return salary_dict
