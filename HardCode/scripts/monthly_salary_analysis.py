
import pandas as pd
from pymongo import MongoClient
import numpy as np
import regex as re
from datetime import datetime, timedelta
#import salary_data
import pytz
import warnings
warnings.filterwarnings('ignore')
def conn():
    connection = MongoClient(
        f"mongodb://root:root123@localhost:27017/?authSource=admin&readPreference=primary&ssl=false",
        socketTimeoutMS=900000)
    return connection

def clean_debit(data, id):
    """This code drops the rows for debited messages and bhanix finance company messages.

        Parameters: DataFrame.

        Output: DataFrame.

    """


    pattern1 = "bhanix"
    pattern2 = "debited"
    d = []
    for i, row in data.iterrows():
        message = row["body"].lower()
        y1 = re.search(pattern1, message)
        y2 = re.search(pattern2, message)

        if y1 is not None or y2 is not None:
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

    data["epf_amount"] = [0] * data.shape[0]
    pattern1 = r"(?:[Ee][Pp][Ff] [Cc]ontribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = r"(?:passbook balance).*?(?:contribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"

    for i, row in data.iterrows():
        m = row["body"].lower()

        y1 = re.search(pattern1, m)
        y2 = re.search(pattern2, m)
        if y1 is not None:
            amount = y1.group(3)
        elif y2 is not None:
            amount = y2.group(3)
        else:
            amount = 0
        data["epf_amount"][i] = float(str(amount).replace(",", ""))
    # logger.info("epf amount calculation completed")
    return data


def epf_to_salary(data, id):
    """This code calculates the salary from the epf amount with formula: epf=12% of salary.

          Parameters: DataFrame.

          Output: DataFrame.

    """
    data["salary"] = [0] * data.shape[0]
    for i in range(0, data.shape[0]):
        data["salary"][i] = (data["epf_amount"][i] * 100) / 12
    # logger.info("Salary Calculation from EPF Amount complete")
    return data

def get_salary(data, id):
    """This code finds the salary from the messages if keyword 'salary' is found.

          Parameters: DataFrame.

          Output: DataFrame.

    """

    #logger = logger_1('Get Salary', id)
    # logger.info('Direct Salary Amount Calculation starts')

    data["direct_sal"] = [0] * data.shape[0]
    pattern1 = r"credited with salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern2 = r"salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*credited"
    pattern3 = r"(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?imps\/salary"
    pattern4 = r"credited.*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?sal.*\/salary"

    for i, row in data.iterrows():
        m = row["body"].lower()

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
        data["direct_sal"][i] = float(str(amount).replace(",", ""))
    # logger.info('Direct salary calculation completes')
    return data


def get_neft_amount(data,id):
    '''This code finds the neft amount from the messages in the DataFrame.

        Parameters: DataFrame.

        Output: DataFrame.

    '''

    data["neft_amount"]=[0]*data.shape[0]

    pattern1="(?:credited).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?neft"
    pattern2="(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?credited.*?neft"
    pattern3="pymt rcvd neft.*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern4="(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?deposited.*neft"

    for i,row in data.iterrows():
        m=str(row['body']).lower()
        y1=re.search(pattern1,m)
        y2=re.search(pattern2,m)
        y3=re.search(pattern3,m)
        y4=re.search(pattern4,m)

        if(y1!=None):
            amount=y1.group(3)
        elif(y2!=None):
            amount=y2.group(3)
        elif(y3!=None):
            amount=y3.group(3)
        elif(y4!=None):
            amount=y4.group(3)
        else:
            amount=0
        data["neft_amount"][i]=float(str(amount).replace(",",""))
    return data


def transaction(id):
    """ This function connects with collection in mongodb database
      Parameters:
      Input : Customer Id
      Output: Dictionary with Parameters:    status(bool):code run successfully or not ,
                                                message(string):success/error ,
                                                df(dataframe): dataframe of transaction data

    """

    connect = conn()
    transaction = connect.messagecluster.transaction

    file1 = transaction.find_one({"cust_id": id})
    x = pd.DataFrame(file1["sms"])

    return x


def extra(id):
    """ This function find rows having epf as keyword in data
      Parameters :
      Input  :  Customer id(int)
      Output :  Returns epf messages dataframe
    """

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
    

    tran = transaction(id)       
    ext = extra(id)
    total = pd.concat([tran, ext], 0)
    total = total.reset_index(drop=True)
    return total

def get_time(data, id):
    """
        This code converts the timestamp from unix format to datetime.

          Parameters: DataFrame.

          Output: DataFrame.

    """


    for i in range(data.shape[0]):
        try:
            x = datetime.strptime(data['timestamp'].values[i], "%Y-%m-%d %H:%M:%S")
            data['timestamp'].values[i] = x
        except:
           
            return {"status": False, "message": "timestamp not converted"}

    return {"status": True, "message": "success", 'data': data}

def updated_message(id):
    connect=conn()
    try:
        
        x1=connect.analysis.salary.find_one({"cust_id":id})
        x2=connect.messagecluster.transaction.find_one({"cust_id":id})
        if x1 != None:
            old_time=x1["modified_at"]
        else:
            old_time =x2["timestamp"] 
        new_time=x2["timestamp"]
        if (old_time<new_time):
            ext=connect.messagecluster.extra.find_one({"cust_id":id})
            file_transaction= pd.DataFrame(x2["sms"])
            file_extra=pd.DataFrame(ext["sms"])
            epf = []
            for i in range(file_extra.shape[0]):
                if re.search("EPFOHO", file_extra["sender"][i]):
                    epf.append(file_extra.values[i])
            epf = pd.DataFrame(epf, columns=['sender', 'body', 'timestamp', 'read'])

            
            total1 = pd.concat([file_transaction, epf], 0)
            total1 = total1.reset_index(drop=True)
            total1.sort_values(by='timestamp')
            for i in range(total1.shape[0]):
                if (total1['timestamp'][i]==new_time):
                    index=i+1
                    total2 = total1.loc[index:]
    
        
        else:
            total2 = merge(id)

        return {"status": True, "message": "Success",'data':total2}    
    except:
        return {"status": False, "message": "id not present",'data':None}


def data(id):
    data1 = updated_message(id)
    if data1["status"]==False: 
        return data1
    else:
        data = pd.DataFrame.from_dict(data1['data'])
        data = clean_debit(data,id)
        result = get_time(data, id)
        if not result['status']:
            return result
        data = result['data']
     
        time = []     
        if data.empty == False:
            grouper = pd.Grouper(key='timestamp', freq='M')
            df_salary = data.groupby(grouper)
            for i, df in df_salary:
                time.append(i)
        else:
            df_salary = None
            time = None


        
        


    return df_salary,time



def main(id):
    df_salary,time = data(id)
    salary_dict = {}
    result = {}
    monthwise = {}
    flag = False
    neft_amt = 0
    neft_time = 0
    try :
        connect = conn()
        r = connect.analysis.salary.find_one({'cust_id':id})
        db = connect.analysis.salary
    except:
        r = None
    if df_salary != None:
        for i,df in df_salary:
            if i in time[::-1][:3]:
                df.reset_index(drop=True, inplace=True)
                df = get_epf_amount(df,id)
                df = epf_to_salary(df, id)
                if df.empty == False:
                    df["salary"] = np.where(df["salary"] >= 7000, df["salary"], 0)
                    epf = df["salary"].max()
                    month = i.month_name(locale = 'English')
                    
                    
                    if epf != 0:
                        
                        for j,row in df.iterrows():
                            if row["salary"]==epf:
                                msg = {'body':row["body"],'sender':row["sender"],'timestamp':row["timestamp"]}
                                
                        salary_dict = {'salary':float(epf),'keyword':'epf','message':msg}
                        monthwise[month] = salary_dict
                        result['cust_id'] = id
                        result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                        result['salary'] = monthwise
                        if r !=None:

                            db.update({'cust_id': id},{"$set":result}, upsert = True)
                            #del r['salary'][next(iter(r['salary']))]

                        else:
                            db.update({'cust_id': id},{"$set":result}, upsert = True)
                    
                    else:
                        df =  get_salary(df,id)
                        sal = df['direct_sal'].max()
                        if sal != 0:
                            for j,row in df.iterrows():
                                if row["direct_sal"]==sal:
                                    msg = {'body':row["body"],'sender':row["sender"],'timestamp':row["timestamp"]}
                            connect = conn()
                            salary_dict = {'salary':float(sal),'keyword':'salary','message':msg}
                            monthwise[month] = salary_dict
                            result['cust_id'] = id
                            result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                            result['salary'] = monthwise
                            
                            if r !=None:
                                
                                db.update({'cust_id': id},{"$set":result}, upsert = True)
                                #del r['salary'][next(iter(r['salary']))]

                            else:
                                db.update({'cust_id': id},{"$set":result}, upsert = True)


                        else:
                            df = get_neft_amount(df,id)
                            df['neft_amount'] = np.where(df["neft_amount"] >= 7000, df["neft_amount"], 0)
                            neft = df['neft_amount'].max()
                            month = i.month_name(locale = 'English')
                            if neft != 0:
                                for j,row in df.iterrows():
                                    if row["neft_amount"]==neft:
                                        msg = {'body':row["body"],'sender':row["sender"],'timestamp':row["timestamp"]}
                                val1 = neft + neft / 5
                                val2 = neft - neft / 5
                                t1 = msg['timestamp'] - timedelta(days=24)
                                t2 = msg['timestamp'] - timedelta(days=37)
                                if flag == False:
                                    neft_amt = neft
                                    flag = True
                                    neft_time = msg['timestamp']
                                    salary_dict = {'salary':0,'keyword':'','message':''}
                                    monthwise[month] = salary_dict
                                    result['cust_id'] = id
                                    result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                                    result['salary'] = monthwise
                                    db.update({'cust_id': id},{"$set":result}, upsert = True)
                                    
                                    
                                else:
                                            
                                    if t2 < neft_time < t1:
                                        if val2 < neft_amt < val1:
                                            salary_dict = {'salary':float(neft),'keyword':'neft','message':msg}
                                            monthwise[month] = salary_dict
                                            result['cust_id'] = id
                                            result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                                            result['salary'] = monthwise
                                            db.update({'cust_id': id},{"$set":result}, upsert = True)
                                            neft_amt = salary_dict['salary']
                                            neft_time = salary_dict['message']['timestamp']
                                            flag = True
                                            
                                            #del r['salary'][next(iter(r['salary']))]
                else:
                    continue
                                    
            else:
                continue
    


    return {'status':True,'message':'Success','result':result['salary']}



print(main(280095))

        
        
        
    
