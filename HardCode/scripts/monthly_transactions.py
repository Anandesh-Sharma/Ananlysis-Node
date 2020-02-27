import pandas as pd
import numpy as np

def get_month_name(month):
  mon =['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
  return mon[month-1]

def monthly_credit_sum(df):
  grouper = pd.Grouper(key='timestamp', freq='M')
  a=(df.groupby(grouper)['credit_amount'].sum())
  k=list(zip(a,a.index))
  sum_month=[(lambda x: (get_month_name(x[1].month),x[0]))(x) for x in k]
  return {'status':True,'message':'success','r':sum_month}

def monthly_credit_average(df):
  grouper = pd.Grouper(key='timestamp', freq='M')
  a=(df.groupby(grouper)['credit_amount'].mean())
  k=list(zip(a,a.index))
  avg_month=[(lambda x: (get_month_name(x[1].month),x[0]))(x) for x in k]
  return {'status':True,'message':'success','r':avg_month}

def monthly_debit_sum(df):
  grouper = pd.Grouper(key='timestamp', freq='M')
  a=(df.groupby(grouper)['debit_amount'].sum())
  k=list(zip(a,a.index))
  sum_month=[(lambda x: (get_month_name(x[1].month),x[0]))(x) for x in k]
  return {'status':True,'message':'success','r':sum_month}

def monthly_debit_average(df):
  grouper = pd.Grouper(key='timestamp', freq='M')
  a=(df.groupby(grouper)['debit_amount'].mean())
  k=list(zip(a,a.index))
  avg_month=[(lambda x: (get_month_name(x[1].month),x[0]))(x) for x in k]
  return {'status':True,'message':'success','r':avg_month}
