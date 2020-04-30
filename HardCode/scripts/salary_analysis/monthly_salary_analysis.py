import pandas as pd
import numpy as np
from HardCode.scripts.Util import logger_1, conn
import regex as re
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz
import warnings
import itertools
from operator import itemgetter

warnings.filterwarnings('ignore')


def clean_debit(data, id):
    """This code drops the rows for debited messages and bhanix finance company messages.

        Parameters: List of Dictionary.

        Output: List of Dictionary.

    """

    pattern1 = "bhanix"
    pattern2 = "debited"
    d = []
    for i in range(len(data)):
        message = data[i]['body'].lower()
        y1 = re.search(pattern1, message)
        y2 = re.search(pattern2, message)

        if y1 is not None or y2 is not None:
            d.append(i)

    for i in d[::-1]:
        data.pop(i)
    # logger.info("Cleaning completed")
    return data


def get_epf_amount(data, id):
    """This code finds the epf(employee provident fund) amount from the messages in the DataFrame.

          Parameters: List of Dictionary.

          Output: List of Dictionary.

    """

    pattern1 = r"(?:[Ee][Pp][Ff] [Cc]ontribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = r"(?:passbook balance).*?(?:contribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"

    for i in range(len(data)):
        m = data[i]["body"].lower()

        y1 = re.search(pattern1, m)
        y2 = re.search(pattern2, m)
        if y1 is not None:
            amount = y1.group(3)
        elif y2 is not None:
            amount = y2.group(3)
        else:
            amount = 0
        data[i]["epf_amount"] = float(str(amount).replace(",", ""))
    # logger.info("epf amount calculation completed")
    return data


def epf_to_salary(data, id):
    """This code calculates the salary from the epf amount with formula: epf=12% of salary.

          Parameters: List of Dictionary.

          Output: List of Dictionary.

    """

    for i in range(len(data)):
        data[i]["salary"] = (data[i]["epf_amount"] * 100) / 15.67
    # logger.info("Salary Calculation from EPF Amount complete")
    return data


def get_salary(data, id):
    """This code finds the salary from the messages if keyword 'salary' is found.

          Parameters: List of Dictionary.

          Output: List of Dictionary.

    """

    # logger = logger_1('Get Salary', id)
    # logger.info('Direct Salary Amount Calculation starts')

    pattern1 = r"credited with salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = r"salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*credited"
    pattern3 = r"(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?imps\/salary"
    pattern4 = r"credited.*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?sal.*\/salary"

    for i in range(len(data)):
        m = data[i]["body"].lower()

        y1 = re.search(pattern1, m)
        y2 = re.search(pattern2, m)
        y3 = re.search(pattern3, m)
        y4 = re.search(pattern4, m)

        if y1 is not None:
            amount = y1.group(3)
        elif y2 is not None:
            amount = y2.group(3)
        elif y3 is not None:
            amount = y3.group(3)
        elif y4 is not None:
            amount = y4.group(3)
        else:
            amount = 0
        data[i]["direct_sal"] = float(str(amount).replace(",", ""))
    # logger.info('Direct salary calculation completes')
    return data


def get_neft_amount(data, id):
    '''This code finds the neft amount from the messages in the DataFrame.

        Parameters: List of Dictionary.

        Output: List of Dictionary.

    '''

    pattern1 = "(?:credited).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?neft"
    pattern2 = "(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?credited.*?neft"
    pattern3 = "pymt rcvd neft.*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern4 = "(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?deposited.*neft"

    for i in range(len(data)):
        m = str(data[i]['body']).lower()
        y1 = re.search(pattern1, m)
        y2 = re.search(pattern2, m)
        y3 = re.search(pattern3, m)
        y4 = re.search(pattern4, m)

        if (y1 != None):
            amount = y1.group(3)
        elif (y2 != None):
            amount = y2.group(3)
        elif (y3 != None):
            amount = y3.group(3)
        elif (y4 != None):
            amount = y4.group(3)
        else:
            amount = 0
        data[i]["neft_amount"] = float(str(amount).replace(",", ""))
    return data


def transaction(id, no_tr_msgs):
    """ This function connects with collection in mongodb database
      Parameters:
      Input : Customer Id
      Output: Dictionary with Parameters:    status(bool):code run successfully or not ,
                                                message(string):success/error ,
                                                df(dataframe): dataframe of transaction data

    """
    connect = conn()
    txn = connect.messagecluster.transaction

    file1 = txn.find_one({"cust_id": id})
    if file1 is None or not file1['sms']:
        no_tr_msgs = True
        return {'status': True, 'cust_id': id, 'message': 'No Transaction messages', 'salary': 0}, no_tr_msgs
    x = file1["sms"]

    return {'cust_id': id, 'status': True, 'message': "success", "df": x}, no_tr_msgs


def extra(id):
    """ This function find rows having epf as keyword in data
      Parameters :
      Input  :  Customer id(int)
      Output :  Returns epf messages dataframe
    """

    connect = conn()
    extra = connect.messagecluster.extra
    file2 = extra.find_one({"cust_id": id})
    y = file2["sms"]

    epf = []
    for i in range(len(y)):
        if re.search("EPFOHO", y[i]["sender"]):
            epf.append(y[i])
    return epf


def merge(id, no_tr_msgs):
    """ This code
     Parameters:
     Input : Customer id(int)
     Output: Dictionary with Parameters:    status(bool):code run successfully or not ,
                                                message(string):success/error ,
                                                df(dataframe): dataframe of merged data
    """

    logger = logger_1('Merge Data', id)
    result, no_tr_msgs = transaction(id, no_tr_msgs)
    if no_tr_msgs:
        return result, no_tr_msgs
    tran = result['df']
    if len(tran) != 0:
        logger.info("Data fetched from Transaction collection")
    else:
        logger.error("No data fetched from Transaction collection")
        no_tr_msgs = True
        return {'status': True, 'message': 'no transaction message', "salary": "0", "cust_id": id}, no_tr_msgs
    ext = extra(id)
    if len(ext) != 0:
        logger.info("Data fetched from Extra collection")
    else:

        logger.info("No data fetched from Extra collection")

    total = tran + ext

    return {'cust_id': id, 'status': True, 'message': 'success', 'total': total}, no_tr_msgs


def data(id, no_tr_msgs):
    data1, no_tr_msgs = merge(id, no_tr_msgs)
    if no_tr_msgs:
        return data1, no_tr_msgs
    else:
        data = data1['total']
        data = clean_debit(data, id)
        if data:
            data.sort(key=lambda x: x['timestamp'])
            dfs = []
            key = lambda datum: datum['timestamp'].rsplit('-', 1)[0]

            for key, group in itertools.groupby(data, key):
                dfs.append({'time': key, 'data': list(group)})

            return {'status': True, 'message': 'Success', 'df': dfs}, no_tr_msgs

        else:
            return {'status': True, 'message': 'No data found', 'df': None}, no_tr_msgs


def main(id):
    '''This code calls all the function to calculate salary of a user based on the messages in dataFrame.
          Input: id(int).
          Output: dictionary with Parameters: status(bool), message(string): Success/exception,
                                             cust_id(int):(id), result(dict): result['salary'],
                                            salary(float):salary of last month.
    '''

    no_tr_msgs = False
    df_data, no_tr_msgs = data(id, no_tr_msgs)
    if no_tr_msgs:
        return {'status': True, 'message': 'No data Found', 'salary': 0, 'cust_id': int(id)}
    if not df_data['status']:
        return {'status': True, 'message': 'No data Found', 'salary': 0, 'cust_id': int(id)}
    if df_data['df']:
        df_salary = df_data['df'][-6:]
    else:
        return {'status': True, 'message': 'No data Found', 'salary': 0, 'cust_id': int(id)}

    salary_dict = {}
    result = {}
    monthwise = {}
    flag = False
    neft_amt = 0
    neft_time = 0

    connect = conn()
    db = connect.analysis.salary

    try:
        for df in df_salary:
            m = datetime.strptime(df['time'], "%Y-%m")
            month = m.strftime("%B")
            df = get_salary(df['data'], id)
            sal = []
            for i in df:
                if i['direct_sal'] != 0:
                    sal.append(i["direct_sal"])
            if len(sal) != 0:
                sal = max(sal)
                for i in df:
                    if i["direct_sal"] == sal:
                        msg = {'body': i["body"], 'sender': i["sender"],
                               'timestamp': str(i["timestamp"])}
                connect = conn()
                salary_dict = {'salary': float(sal), 'keyword': 'salary', 'message': msg}
                monthwise[month] = salary_dict
                result['cust_id'] = id
                result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                result['salary'] = monthwise
                db.update({'cust_id': id}, {"$set": result}, upsert=True)

            else:
                df = get_epf_amount(df, id)
                df = epf_to_salary(df, id)
                vals = []
                for i in df:
                    if i['salary'] >= 7000:
                        vals.append(i['salary'])

                if len(vals) != 0:
                    epf = max(vals)
                    for i in df:
                        if i["salary"] == epf:
                            msg = {'body': i["body"], 'sender': i["sender"], 'timestamp': str(i["timestamp"])}

                    salary_dict = {'salary': round(float(epf), 2), 'keyword': 'epf', 'message': msg}
                    monthwise[month] = salary_dict
                    result['cust_id'] = id
                    result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                    result['salary'] = monthwise
                    db.update({'cust_id': id}, {"$set": result}, upsert=True)

                else:
                    df = get_neft_amount(df, id)
                    amnt = []
                    for i in df:
                        if i['neft_amount'] >= 7000:
                            amnt.append(i['neft_amount'])

                    if len(amnt) != 0:
                        neft = max(amnt)
                        for i in df:
                            if i["neft_amount"] == neft:
                                msg = {'body': i["body"], 'sender': i["sender"],
                                       'timestamp': str(i["timestamp"])}
                        val1 = neft + neft / 5
                        val2 = neft - neft / 5
                        t1 = datetime.strptime(msg['timestamp'], "%Y-%m-%d %H:%M:%S") - timedelta(days=24)
                        t2 = datetime.strptime(msg['timestamp'], "%Y-%m-%d %H:%M:%S") - timedelta(days=37)
                        if flag == False:
                            neft_amt = neft
                            flag = True
                            neft_time = datetime.strptime(msg['timestamp'], "%Y-%m-%d %H:%M:%S")
                            salary_dict = {'salary': 0, 'keyword': '', 'message': ''}
                            monthwise[month] = salary_dict
                            result['cust_id'] = id
                            result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                            result['salary'] = monthwise
                            db.update({'cust_id': id}, {"$set": result}, upsert=True)


                        else:

                            if t2 < neft_time < t1:
                                if val2 < neft_amt < val1:
                                    salary_dict = {'salary': float(neft), 'keyword': 'neft', 'message': msg}
                                    monthwise[month] = salary_dict
                                    result['cust_id'] = id
                                    result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                                    result['salary'] = monthwise
                                    db.update({'cust_id': id}, {"$set": result}, upsert=True)
                                    neft_amt = salary_dict['salary']
                                    neft_time = datetime.strptime(salary_dict['message']['timestamp'],
                                                                  "%Y-%m-%d %H:%M:%S")
                                    flag = True

                                    # del r['salary'][next(iter(r['salary']))]


                    else:
                        salary_dict = {'salary': 0, 'keyword': '', 'message': 'no salary found'}
                        monthwise[month] = salary_dict
                        result['cust_id'] = id
                        result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                        result['salary'] = monthwise
                        db.update({'cust_id': id}, {"$set": result}, upsert=True)

        last_month = list(result['salary'].keys())[-1]
        salary = result['salary'][last_month]['salary']

        return {'status': True, 'message': 'Success', 'cust_id': int(id), 'result': result['salary'],
                'salary': float(salary)}
    except Exception as e:
        return {'status': False, 'message': str(e), 'cust_id': int(id), 'salary': 0}
