import numpy as np
import os
import pandas as pd
import re
import pytz
from datetime import datetime
from pymongo import MongoClient

from HardCode.scripts.Util import conn

pd.options.mode.chained_assignment = None


def get_extracted_data(data):

    df = pd.DataFrame(columns=['amount', 'status', 'available_card_limit', 'current_outstanding_amt',
                           'minimum_due_amt', 'total_due_amt', 'bank_name'])
                           
    df['total_due_amt'] = [0] * data.shape[0]
    df['minimum_due_amt'] = [0] * data.shape[0]
    df['available_card_limit'] = [0] * data.shape[0]
    df['amount'] = [0] * data.shape[0]
    df['current_outstanding_amt'] = [0] * data.shape[0]
    df['status'] = ["not defined"] * data.shape[0]

    pattern_1 = r'(?:total|tot)\s(?:amt|amount).*(?:rs\.?|inr)\s?\s?([0-9,]+).*(?:minimum|min\.?)\s(?:amt|amount).*(' \
                r'?:rs\.?|inr)\s?\s?([0-9,]+[.]?[0-9]+)'
    pattern_2 = r'(?:minimum|min\.?)\s(?:amt|amount).*(?:rs\.?|inr)\s?\s?([0-9.,]+).*(?:total|tot)\s(?:amt|amount).*(' \
                r'?:rs\.?|inr)\s?\s?([0-9,]+[.]?[0-9]+)'
    pattern_3 = r'(?:total|tot)\s(?:amt|amount).*(?:rs\.?|inr)\s?\s?([0-9,]+[.]?[0-9]+)'
    pattern_4 = r'(?:minimum|min\.?)\s(?:amt|amount).*(?:rs\.?|inr)\s?\s?([0-9,]+[.]?[0-9]+)'
    pattern_5 = r'.*forward.*receiving\s?rs\.?\s?([0-9,?]+[.]?[0-9]+).*credit\scard.*'
    pattern_6 = r'.*not\sreceived\spayment.*credit\scard.*rs\.?\s?([0-9,]+[.]?[][0-9]+).*'
    pattern_7 = r'.*unable.*overdue\s(?:payment|pymt).*rs\.?\s?([0-9,]+[.]?[0-9]+).*credit\scard.*'
    pattern_8 = r'.*outstanding.*(?:rs\.?|inr)\s?\s?([0-9]+[.]?[0-9]+).*(?:minimum|min).*(?:rs\.?|inr)\s?\s?([0-9]+[' \
                r'.]?[0-9]+)'
    pattern_9 = r'.*(?:statement|stmt).*(?:rs\.?|inr)\s?\s?([0-9]+[.]?[0-9]+).*due\sdate\s([0-9]{1,' \
                r'2}[-](?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-][0-9]{1,2}).*(?:minimum|min\.?).*(' \
                r'?:rs\.?|inr)\s?\s?([0-9]+[.]?[0-9]).*'
    pattern_10 = r'(?:payment|pymt|pymnt|transaction|trxn|txn|charge).*(?:rs\.?|inr\.?)\s?\s?([0-9,]+[.]?[0-9]+).*(' \
                 r'?:available|avl)\s(?:limit|lmt).*(?:rs\.?|inr\.?)\s?\s?([-]?[0-9,]+[.]?[0-9]+)'
    pattern_11 = r'.*spent\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:available|avl)\s(?:balance|bal).*(' \
                 r'?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:current|curr).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*'
    pattern_12 = r'(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*debited.*(?:available|avbl).*(?:limit|lmt).*(?:rs\.?|inr)\s?(' \
                 r'[0-9,]+[.]?[0-9]+)'
    pattern_13 = r'(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*spent.*card.*(?:available|avl\.?).*(' \
                 r'?:limit|lim\.?|bal\.?|balance).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'
    pattern_14 = r'(?:payment|pymt|pymnt|transaction|trxn|txn|charge).*(?:rs\.?|inr)\s?\s?([0-9,' \
                 r']+[.]?[0-9]+).*received.*(?:available|avl).*(?:limit|lmt\.?).*(?:rs\.?)\s?\s?([-]?[0-9,' \
                 r']+[.]?[0-9]+)'
    pattern_15 = r'(?:payment|pymt|trxn|txn|transaction)\sof\s(?:rs\.?|inr)\s?\s?([0-9,]+[.]?[0-9]+).*due.*(' \
                 r'?:minimum|min\.?).*(?:rs\.?|inr)\s?\s?([0-9]+[.]?[0-9]+)'
    pattern_16 = r'(?:payment|pymt|trnx|transaction|txn)\sof\s(?:rs\.?|inr)\s?\s?([0-9,]+[.]?[0-9]?).*received.*'

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        sender = str(data['Sender-Name'][i]).lower()
        if sender == 'icicib':
            df['bank_name'][i] = 'icici'
        elif sender == 'sbicrd' or sender == 'sbiinb' or sender == 'cmntri':
            df['bank_name'][i] = 'sbi'
        elif sender == 'kbankt' or sender == 'kotakb':
            df['bank_name'][i] = 'kotak'
        elif sender == 'indusb':
            df['bank_name'][i] = 'indusind'
        elif sender == 'rblcrd' or sender == 'rblbnk':
            df['bank_name'][i] = 'rbl'
        elif sender == 'hdfcbk' or sender == 'hdfccc':
            df['bank_name'][i] = 'hdfc'
        elif sender == 'axisbk':
            df['bank_name'][i] = 'axis'
        else:
            df['bank_name'][i] = 'not mentioned'

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)
        matcher_7 = re.search(pattern_7, message)
        matcher_8 = re.search(pattern_8, message)
        matcher_9 = re.search(pattern_9, message)
        matcher_10 = re.search(pattern_10, message)
        matcher_11 = re.search(pattern_11, message)
        matcher_12 = re.search(pattern_12, message)
        matcher_13 = re.search(pattern_13, message)
        matcher_14 = re.search(pattern_14, message)
        matcher_15 = re.search(pattern_15, message)
        matcher_16 = re.search(pattern_16, message)

        if matcher_1 is not None:
            df['total_due_amt'][i] = float(str(matcher_1.group(1)).replace(",", ""))
            df['minimum_due_amt'][i] = float(str(matcher_1.group(2)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_2 is not None:
            df['total_due_amt'][i] = float(str(matcher_2.group(2)).replace(",", ""))
            df['minimum_due_amt'][i] = float(str(matcher_2.group(1)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_3 is not None:
            df['total_due_amt'][i] = float(str(matcher_3.group(1)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_4 is not None:
            df['minimum_due_amt'][i] = float(str(matcher_4.group(1)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_5 is not None:
            df['minimum_due_amt'][i] = float(str(matcher_5.group(1)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_6 is not None:
            df['minimum_due_amt'][i] = float(str(matcher_6.group(1)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_7 is not None:
            df['total_due_amt'][i] = float(str(matcher_7.group(1)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_8 is not None:
            df['total_due_amt'][i] = float(str(matcher_8.group(1)).replace(",", ""))
            df['minimum_due_amt'][i] = float(str(matcher_8.group(2)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_9 is not None:
            df['amount'][i] = float(str(matcher_9.group(1)).replace(",", ""))
            df['minimum_due_amt'][i] = float(str(matcher_9.group(3)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_10 is not None:
            df['amount'][i] = float(str(matcher_10.group(1)).replace(",", ""))
            df['available_card_limit'][i] = float(str(matcher_10.group(2)).replace(",", ""))
            df['status'][i] = 'transaction'
        elif matcher_11 is not None:
            df['amount'][i] = float(str(matcher_11.group(1)).replace(",", ""))
            df['available_card_limit'][i] = float(str(matcher_11.group(2)).replace(",", ""))
            df['current_outstanding_amt'][i] = float(str(matcher_11.group(3)).replace(",", ""))
            df['status'][i] = 'transaction'
        elif matcher_12 is not None:
            df['amount'][i] = float(str(matcher_12.group(1)).replace(",", ""))
            df['available_card_limit'][i] = float(str(matcher_12.group(2)).replace(",", ""))
            df['status'][i] = "transaction"
        elif matcher_13 is not None:
            df['amount'][i] = float(str(matcher_13.group(1)).replace(",", ""))
            df['available_card_limit'][i] = float(str(matcher_13.group(2)).replace(",", ""))
            df['status'][i] = "transaction"
        elif matcher_14 is not None:
            df['amount'][i] = float(str(matcher_14.group(1)).replace(",", ""))
            df['available_card_limit'][i] = float(str(matcher_14.group(2)).replace(",", ""))
            df['status'][i] = "transaction"
        elif matcher_15 is not None:
            df['amount'][i] = float(str(matcher_15.group(1)).replace(",", ""))
            df['minimum_due_amt'][i] = float(str(matcher_15.group(2)).replace(",", ""))
            df['status'][i] = "due"
        elif matcher_16 is not None:
            df['amount'][i] = float(str(matcher_16.group(1)).replace(",", ""))
            df['status'][i] = "transaction"
        else:
            pass
    return df


def sms_header_splitter(data):
    data['Sender-Name'] = np.nan
    for i in range(len(data)):
        data['sender'][i] = data['sender'][i].replace('-', '')
        data['sender'][i] = data['sender'][i][2:]
        data['Sender-Name'][i] = data['sender'][i]
    data.drop(['sender'], axis=1, inplace=True)
    return data


def get_cc_limit(user_id):
    connect = conn()
    df  = connect.analysis.parameters
    parameters = {}
    result = {}
    output = {}
    try:
        db = connect.messagecluster.creditcard
        file1 = db.find_one({"cust_id": user_id})
        if file1:
            if len(file1['sms']) != 0:
                data = pd.DataFrame(file1["sms"])
                sms_header_splitter(data)

                final_df = get_extracted_data(data)
                bank = final_df.groupby('bank_name')
                x = bank['available_card_limit'].max()
                x = x.to_dict()
                delete = [key for key in x if key == 'not mentioned']
                for key in delete:
                    del x[key]
                result = x
        status = True
        msg = 'success'
    except BaseException as e:
        status = False
        msg = str(e)
        print(f"error in credit card limit check: {e}")
    finally:

        parameters['cust_id'] = user_id
        df.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                   'parameters.credit_card':result,}}, upsert=True)
        return {'status':status,'message':'success'}
