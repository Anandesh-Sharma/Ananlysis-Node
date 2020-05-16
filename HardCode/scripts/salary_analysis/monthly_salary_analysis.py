from HardCode.scripts.Util import logger_1, conn
import regex as re
import itertools
import warnings
from datetime import datetime, timedelta
import pytz

warnings.filterwarnings('ignore')


def transaction_msg(user_id):
    logger = logger_1("balance sheet msgs", user_id)
    logger.info("fetching data from balance sheet")
    connect = conn()
    msgs = connect.analysis.balance_sheet.find_one({'cust_id': user_id})
    deposited_msg = connect.messagecluster.salary.find_one({'cust_id': user_id})
    sal_modified = connect.analysis.salary.find_one({'cust_id': user_id})
    final = []


    if sal_modified is None:
        new_user = True
    else:
        new_user = False

    index = 0
    if not new_user:
        max_time = msgs['max_timestamp']
        sal_modified = sal_modified['modified_at']
        if sal_modified < max_time:
            for i in range(len(msgs['sheet'])):
               if msgs['sheet'][i]['timestamp'] == sal_modified:
                   index = i + 1
                   break


    if deposited_msg:
        deposited = deposited_msg['deposited']
    else:
        deposited = []
    if not msgs['sheet'] and not deposited:
        return {'status': True, 'cust_id': user_id, 'message': 'No Transaction messages', 'salary': 0}
    elif not msgs['sheet'] and deposited:
        msgs = deposited
    else:
        msgs = msgs['sheet'][index:]
        msgs = msgs + deposited

    for m in range(len(msgs)):
        picked = {'body': msgs[m]['body'], 'sender': msgs[m]['sender'], 'timestamp': msgs[m]['timestamp'],
                  'read': msgs[m]['read'], 'Credit Amount': msgs[m]['Credit Amount']}
        final.append(picked)
    logger.info("data fetched from balance sheet")
    return final



def get_salary_msg(data):
    salary = []
    for i in range(len(data)):
        m = data[i]['body'].lower()
        if re.search('salary', m):
            salary.append(data[i])
    return salary


def get_neft_amount(data):
    neft = []
    for i in range(len(data)):
        m = data[i]['body'].lower()
        if re.search('salary', m):
            neft.append(data[i])
    return neft


def get_epf_msg(data):
    epf = []
    for i in range(len(data)):
        m = data[i]['body'].lower()
        if re.search('contribution of', m):
            epf.append(data[i])

    return epf


def get_epf_amount(user_id):
    """This code finds the epf(employee provident fund) amount from the messages in the DataFrame.

          Parameters: List of Dictionary.

          Output: List of Dictionary.

    """
    logger = logger_1("get epf amount", user_id)
    logger.info("fetching epf msgs")
    connect = conn()
    extra = connect.messagecluster.salary.find_one({"cust_id": user_id})
    if extra:
        data = extra["epf"]
    else:
        data = []

    pattern1 = r"(?:[Ee][Pp][Ff] [Cc]ontribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = r"(?:passbook balance).*?(?:contribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    if data:
        for i in range(len(data)):
            m = data[i]["body"].lower()

            y1 = re.search(pattern1, m)
            y2 = re.search(pattern2, m)
            if y1 is not None:
                val = y1.group(3)
                amount = (float(str(val).replace(",", "")) * 100) / 15.67
            elif y2 is not None:
                val = y2.group(3)
                amount = (float(str(val).replace(",", "")) * 100) / 15.67
            else:
                amount = 0
            data[i]["Credit Amount"] = float(amount)
    logger.info("epf amount calculation completed")
    return data


def sorted_data(data,user_id):
    logger = logger_1("sorted data", user_id)
    logger.info("sorting data")
    if data:
        data.sort(key=lambda x: x['timestamp'])
        dfs = []
        key = lambda datum: datum['timestamp'].rsplit('-', 1)[0]

        for key, group in itertools.groupby(data, key):
            dfs.append({'time': key, 'data': list(group)})

        return {'status': True, 'message': 'Success', 'df': dfs}

    else:
        return {'status': True, 'message': 'No data found', 'df': None}


def salary_main(user_id):
    logger = logger_1("main function salary", user_id)
    result = {}
    monthwise = {}
    flag = False
    neft_amt_1 = 0
    neft_time = 0

    connect = conn()
    db = connect.analysis.salary
    trans = transaction_msg(user_id)
    epf = get_epf_amount(user_id)
    if isinstance(trans,dict) and not epf:
        return {'status': True, 'message': "no messages", 'cust_id': int(user_id), 'salary': 0}
    elif isinstance(trans,dict) and epf :
        bal_sheet = epf
    else:
        bal_sheet = trans + epf

    try:
        if bal_sheet:
            sorted = sorted_data(bal_sheet,user_id)
            if sorted['df']:
                for df in sorted['df']:
                    m = datetime.strptime(df['time'], "%Y-%m")
                    month = m.strftime("%B")
                    data = df['data']
                    salary = get_salary_msg(data)

                    sal = []

                    if salary:
                        for j in range(len(salary)):
                            if salary[j]['Credit Amount'] >= 7000:
                                sal.append(salary[j]['Credit Amount'])

                    if sal:
                        sal_amt = max(sal)
                        for i in range(len(salary)):
                            if salary[i]['Credit Amount'] == sal_amt:
                                msg = {'body': salary[i]["body"], 'sender': salary[i]["sender"],
                                       'timestamp': str(salary[i]["timestamp"])}

                        salary_dict = {'salary': float(sal_amt), 'keyword': 'salary', 'message': msg}
                        monthwise[month] = salary_dict
                        result['cust_id'] = user_id
                        result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                        result['salary'] = monthwise
                        db.update({'cust_id': user_id}, {"$set": result}, upsert=True)
                        logger.info("salary found from salary keyword")

                    else:
                        epf_msg = get_epf_msg(data)
                        epf = []
                        if epf_msg:
                            for j in range(len(epf_msg)):
                                if epf_msg[j]['Credit Amount'] > 0:
                                    epf.append(epf_msg[j]['Credit Amount'])

                        if epf:
                            epf_amt = max(epf)
                            for i in range(len(epf_msg)):
                                if epf_msg[i]['Credit Amount'] == epf_amt:
                                    msg = {'body': epf_msg[i]["body"], 'sender': epf_msg[i]["sender"],
                                           'timestamp': str(epf_msg[i]["timestamp"])}

                            salary_dict = {'salary': round(float(epf_amt), 2), 'keyword': 'epf', 'message': msg}
                            monthwise[month] = salary_dict
                            result['cust_id'] = user_id
                            result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                            result['salary'] = monthwise
                            db.update({'cust_id': user_id}, {"$set": result}, upsert=True)
                            logger.info("salary found from epf")


                        else:
                            neft_msg = get_neft_amount(data)
                            neft = []
                            if neft_msg:
                                for j in range(len(neft_msg)):
                                    if neft_msg[j]['Credit Amount'] >= 7000:
                                        neft.append(neft_msg[j]['Credit Amount'])
                            if neft:
                                neft_amt = max(neft)
                                for i in range(len(neft_msg)):
                                    if neft_msg[i]['Credit Amount'] == neft_amt:
                                        msg = {'body': neft_msg[i]["body"], 'sender': neft_msg[i]["sender"],
                                               'timestamp': str(neft_msg[i]["timestamp"])}

                                val1 = neft_amt + neft_amt / 5
                                val2 = neft_amt - neft_amt / 5
                                t1 = datetime.strptime(msg['timestamp'], "%Y-%m-%d %H:%M:%S") - timedelta(
                                    days=24)
                                t2 = datetime.strptime(msg['timestamp'], "%Y-%m-%d %H:%M:%S") - timedelta(
                                    days=37)
                                if flag == False:
                                    neft_amt_1 = neft_amt
                                    flag = True
                                    neft_time = datetime.strptime(msg['timestamp'], "%Y-%m-%d %H:%M:%S")
                                    salary_dict = {'salary': 0, 'keyword': '', 'message': ''}
                                    monthwise[month] = salary_dict
                                    result['cust_id'] = user_id
                                    result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                                    result['salary'] = monthwise
                                    db.update({'cust_id': user_id}, {"$set": result}, upsert=True)

                                else:
                                    if t2 < neft_time < t1:
                                        if val2 < neft_amt_1 < val1:
                                            salary_dict = {'salary': float(neft_amt), 'keyword': 'neft',
                                                           'message': msg}
                                            monthwise[month] = salary_dict
                                            result['cust_id'] = user_id
                                            result['modified_at'] = str(
                                                datetime.now(pytz.timezone('Asia/Kolkata')))
                                            result['salary'] = monthwise
                                            db.update({'cust_id': user_id}, {"$set": result}, upsert=True)
                                            neft_amt_1 = salary_dict['salary']
                                            neft_time = datetime.strptime(salary_dict['message']['timestamp'],"%Y-%m-%d %H:%M:%S")
                                            flag = True
                                            logger.info("salary found from neft keyword")
                            else:
                                salary_dict = {'salary': 0, 'keyword': '', 'message': 'no salary found'}
                                monthwise[month] = salary_dict
                                result['cust_id'] = user_id

                                result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                                result['salary'] = monthwise
                                db.update({'cust_id': user_id}, {"$set": result}, upsert=True)

                last_month = list(result['salary'].keys())[-1]
                last_salary = result['salary'][last_month]['salary']

        logger.info("salary analysis completed")
        return {'status': True, 'message': 'Success', 'cust_id': int(user_id), 'result': result['salary'],
                'salary': float(last_salary)}
        
    except BaseException as e:
        print(e)
        import traceback
        traceback.print_tb(e.__traceback__)
        return {'status': False, 'message': str(e), 'cust_id': int(user_id), 'salary': 0}






