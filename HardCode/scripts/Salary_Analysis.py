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

def clean_debit(data):
  '''This code drops the rows for debited messages and bhanix finance company messages.
  
      Parameters: DataFrame.
      
      Output: DataFrame.
      
      '''
  pattern1="bhanix"
  pattern2="debited"
  d=[]
  for i,row in data.iterrows():
    message = data["body"][i].lower()
    y1=re.search(pattern1,message)
    y2=re.search(pattern2,message)

    if y1!=None or y2!=None:
      d.append(i)

  data.drop(d,inplace=True)
  data.reset_index(drop=True,inplace=True)
  
  return data
    

def get_credit_amount(data):
    '''
    This code finds the credited amount from the messages in a DataFrame.
      
          Parameters: DataFrame.
          
          Output: DataFrame.
          
          '''
    data['credit_amount']=[0]*data.shape[0]
    pattern_2 = '(?i)credited.*?(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern_1 = '(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?).*?credited'
    #pattern_3 = "credited with salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
    pattern_4 = '(?i)(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?).*?deposited'
    pattern_5 = '(?i)(?:(?:rs|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?).*?received'
    pattern_6 = '(?i)received.*?(?:(?:rs|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    #pattern_7 = "salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*credited"
    #pattern_debit_1 = '(?i)debited(.*)?(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern_debit_2 = 'credited to beneficiary'
    
    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1,message)
        matcher_2 = re.search(pattern_2,message)
        #matcher_3 = re.search(pattern_3,message)
        matcher_4 = re.search(pattern_4,message)
        matcher_5 = re.search(pattern_5,message)
        matcher_6 = re.search(pattern_6,message)
     
        amount=0
        if matcher_1!= None:
            #matcher_debit_1 = re.search(pattern_debit_1,message)
            matcher_debit_2 = re.search(pattern_debit_2,message)
            if(matcher_debit_2!=None):
                amount=0
            
            else:
                amount=matcher_1.group(1)

        elif matcher_2!= None:
            amount=matcher_2.group(1)
        

        elif matcher_4!= None:
            amount=matcher_4.group(1)
        elif matcher_5!= None:
            amount=matcher_5.group(1)
        elif matcher_6!= None:
            amount=matcher_6.group(1)
      

        else:
            amount=0
        try:
            data['credit_amount'][i]=float(str(amount).replace(",",""))
        except Exception as e:
            print(e)
            print(i+2)
            print(str(amount).replace(",",""))
    return data

def get_epf_amount(data):
  '''This code finds the epf(employee provident fund) amount from the messages in the DataFrame.
    
        Parameters: DataFrame.
        
        Output: DataFrame.
        
        '''

  data["epf_amount"]=[0]*data.shape[0]
  pattern1="(?:[Ee][Pp][Ff] [Cc]ontribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
  pattern2="(?:passbook balance).*?(?:contribution of).*?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
 

  for i,row in data.iterrows():
    m=row["body"].lower()

    y1=re.search(pattern1,m)
    y2=re.search(pattern2,m)
    if(y1!=None):
      amount=y1.group(3)
    elif(y2!=None):
      amount=y2.group(3)
    else:
      amount=0
    data["epf_amount"][i]=float(str(amount).replace(",",""))
  return data


def epf_to_salary(data,column):
  '''This code calculates the salary from the epf amount with formula: epf=12% of salary.
    
        Parameters: DataFrame.
        
        Output: DataFrame.
        
        '''
  
  data["salary"]=[0]*data.shape[0]
  for i in range(0,data.shape[0]):
    data["salary"][i]=(data[column][i]*100)/12
  return data


def get_salary(data):
  '''This code finds the salary from the messages if keyword 'salary' is found.
    
        Parameters: DataFrame.
        
        Output: DataFrame.
        
        '''
  data["direct_sal"]=[0]*data.shape[0]
  pattern1="credited with salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?))"
  pattern2="salary of ?(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*credited"
  pattern3="(((?:[Rr][sS]|inr)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)).*?imps\/salary"
 

  for i,row in data.iterrows():
    m=row["body"].lower()

    y1=re.search(pattern1,m)
    y2=re.search(pattern2,m)
    y3=re.search(pattern3,m)
    if(y1!=None):
      amount=y1.group(3)
    elif(y2!=None):
      amount=y2.group(3)
    elif(y3!=None):
      amount=y3.group(3)
    else:
      amount=0
    data["direct_sal"][i]=float(str(amount).replace(",",""))
  return data


def get_time(data):
  '''This code converts the timestamp from unix format to datetime.
    
        Parameters: DataFrame.
        
        Output: DataFrame.
        
        '''
  for i in range(data.shape[0]):
    try:
      x = datetime.strptime(data['timestamp'].values[i],"%Y-%m-%d %H:%M:%S")
      data['timestamp'].values[i]=x
    except:
      print("timestamp could not be converted at "+i)

  return data

def salary_check(data):
  '''This code calls all the function to calculate salary of a user based on the messages in dataFrame.
    
        Parameters: DataFrame.
        
        Output: DataFrame.
        
        '''
  data=clean_debit(data)
  grouper = pd.Grouper(key='timestamp', freq='M')
  data = get_time(data)
  var1=True
  var2=True
  salary=0
  
  
  data = get_epf_amount(data)
  data = epf_to_salary(data,"epf_amount")
  df_salary=data.groupby(grouper)['salary'].max()
    
  try:
    if (df_salary[-1]!=0) | (df_salary!="nan"):
      salary=df_salary[-1]
    elif (df_salary[-2]!=0) | (df_salary!="nan"):
      salary=df_salary[-2]
      
      
      var1=False
      var2=False
    #print(salary)
  except:
    salary=None

  try:  
    if var1:
      data = get_salary(data)
      df_d_salary=data.groupby(grouper)['direct_sal'].max()
      if (df_d_salary[-1]!=0) | (df_d_salary!="nan"):
        salary=df_d_salary[-1]
      elif (df_d_salary[-2]!=0) | (df_d_salary!="nan"):
        salary=df_d_salary[-2]
          
        var2=False

  except:
    salary=None

  try:

    if var2:
      data = get_credit_amount(data)
      
      data["credit_amount"]=np.where(data["credit_amount"]>=10000,data["credit_amount"],0)
      
      df_credit=data.groupby(grouper)['credit_amount'].max()

      df_final_sal=pd.DataFrame(df_credit.tail())
      
      #print(df_final_sal)
      
      if df_final_sal.shape[0]>1:
        if((df_final_sal["credit_amount"][-1]!=0) and (df_final_sal["credit_amount"][-2]!=0)):
        
          real_money = list(df_final_sal['credit_amount'])[::-1]
          #print(real_money)
          month=[w.month for w in list(df_final_sal.index)][::-1]
          a1=True
          a2=False
          #a3=False
          list_date=[]
          for i in range(data.shape[0]):
          
            if a1:
              if data['credit_amount'][i]==real_money[0]:
                list_date.append(data['timestamp'][i])
                a1=False
                a2=True
            if a2:
              if data['credit_amount'][i]==real_money[1]:
                #if data['timestamp'][i].month == month[1]:    
                list_date.append(data['timestamp'][i])
                #a2=False
                #a3=True
                break
            
          #print(list_date)
          time1=list_date[0]-timedelta(days=26)
          time2=list_date[0]-timedelta(days=34)
          val1=df_final_sal["credit_amount"][-1]+df_final_sal["credit_amount"][-1]/4
          val2=df_final_sal["credit_amount"][-1]-df_final_sal["credit_amount"][-1]/4
          
          
          if (time2<list_date[1]<time1):
            if (val2<df_final_sal["credit_amount"][-2]<val1):
              salary=(df_final_sal["credit_amount"][-1]+df_final_sal["credit_amount"][-2])/2
            else:
              
              return
  except:
    salary=None        

            
  return salary

def conn():
    connection = MongoClient("mongodb://superadmin:rock0004@13.76.177.87:27017/?authSource=admin&readPreference=primary&ssl=false",maxPoolSize=200)
    return connection

def transaction(id):
  connect=conn()
  transaction = connect.messagecluster.transaction
  file1=transaction.find_one({"_id":id})
  x = pd.DataFrame(file1)
  df1 = pd.DataFrame()
  full = pd.DataFrame()
  for i in range(x.shape[0]):
    #print(x['sms'][i])
    p = pd.DataFrame(x['sms'][i],index = [0])
    df1 = pd.concat([df1,p],axis=0)
  df1 = df1.reset_index(drop = True)
  full=pd.concat([x,df1], axis = 1)
  full=full.drop(["sms"],1)
  return full

def extra(id):
  connect=conn()
  extra = connect.messagecluster.extra
  file2=extra.find_one({"_id":id})
  y = pd.DataFrame(file2)
  df2 = pd.DataFrame()
  full2 = pd.DataFrame()
  for i in range(y.shape[0]):
    #print(x['sms'][i])
    p = pd.DataFrame(y['sms'][i],index = [0])
    df2 = pd.concat([df2,p],axis=0)
  df2 = df2.reset_index(drop = True)
  full2=pd.concat([y,df2], axis = 1)
  full2=full2.drop(["sms"],1)

  epf=[]
  for i in range(full2.shape[0]):
    if re.search("EPFOHO",full2["sender"][i]):
      epf.append(full2.values[i])
  epf=pd.DataFrame(epf,columns=['_id','sender','body','timestamp','read'])
  return epf

def merge(id):
  tran=transaction(id)
  ext=extra(id)
  total=pd.concat([tran,ext],0)
  total=total.reset_index(drop = True)
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
        salary(int/Nonetype)     : salary of the customer found then int otherwise Nonetype.
        
        '''
  salary_status={}
  user_id=id
  try:
    merged=merge(id)
    salary = salary_check(merged)
    salary_status["SALARY"]=salary
       
    if (salary==0) | (salary==None):
      salary_status["SALARY"]=None
      status=False
      message="No Salary Found"
      
    else: 
      status=True
      message="SUCCESS"
  except Exception as e:
       
    status=False
    message="ERROR"
    salary_status["SALARY"]=None

  salary_status["USER_ID"]=user_id
  salary_status["STATUS"]=status
  salary_status["MESSAGE"]=message

  return salary_status

def convert_json(data, name):
    obj = {"_id": int(name), "SALARY": []}
    for i in tqdm(range(data.shape[0])):
        salary = {"SALARY": data['SALARY'][i]}
        obj['SALARY'].append(salary)
    return obj

def salary_analysis(id):
    salary_dict=customer_salary(id)
    sal_df = pd.DataFrame(salary_dict,index = [0])
    json_sal = convert_json(sal_df, id)
    connect = conn()
    db = connect.messagecluster.salary
    db.insert_one(json_sal)
    connect.close()

