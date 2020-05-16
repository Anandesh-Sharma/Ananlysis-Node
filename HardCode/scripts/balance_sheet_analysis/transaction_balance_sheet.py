from .transaction_analysis import process_data
from .monthly_transactions import monthly_credit_sum, monthly_debit_sum
from HardCode.scripts.balance_sheet_analysis.Validation2 import *
from datetime import datetime
import pytz
import json
from HardCode.scripts.Util import conn, logger_1, convert_json_balanced_sheet# convert_json_balanced_sheet_empty


def create_transaction_balanced_sheet(user_id):
    logger = logger_1("create transaction_balanced_sheet", -1)
    if not isinstance(user_id, int):
        logger.error('Type Error:user_id not int type')
        return {'status': False, 'message': 'Type Error:user_id not int type'}
    pd.options.mode.chained_assignment = None
    logger = logger_1("create transaction_balanced_sheet", user_id)

    logger.info('Connecting to db')
    try:
        client = conn()
        file1 = client.messagecluster.transaction.find_one({"cust_id": user_id})
    except:
        logger.exception("Data for balanced sheet not found")
        return {'status': False, 'message': 'data for balanced sheet not found'}

    logger.info('conncection successful')

    if file1 is None:
        logger.error("file doesn't exist in database")
        return {'status': False, 'message': "file doesn't exist in database"}
    df = pd.DataFrame(file1['sms'])
    # do something for updation
    old_balance_sheet = client.analysis.balance_sheet.find_one({"cust_id": user_id})
    if old_balance_sheet is None:
        new = True
    else:
        new = False
    if not new:
        old_timestamp = old_balance_sheet["max_timestamp"]
        p = True
        for i in range(df.shape[0]):
            if df['timestamp'][i] == old_timestamp:
                index = i + 1
                p = False
                break
        if p:
            index = 0
        df = df.loc[index:]
        if df.shape[0] == 0:
            return {'status': True, 'message': 'success'}  # do something
    # doing something
    logger.info('Converting file to dataframe')
    if df.shape[0] == 0:
        r={}
        r['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
        r['cust_id'] = user_id
        r['max_timestamp']="2000-03-30 04:10:24"
        r['final_credit']='mar/2000'
        r['credit']=[]
        r['debit']=[]
        r['sheet']=[]
        try:
            client.analysis.balance_sheet.update({'cust_id': user_id}, {"$set": r}, upsert=True)
            logger.info('balanced sheet found and saved')
            return {'status':True,'message':'success'}
        except BaseException as e:
            return {'status':False,'message':str(e)}
    logger.info('Conversion Successful')
    logger.info('Starting to process data')
    result = process_data(df, user_id)

    if not result['status']:
        logger.exception(result['message'])
        return result
    logger.info('Processing of data Successful')

    df = result['df']

    logger.info('Starting validation')
    logger.info('Checking Upi ref number')
    result = upi_ref_check(df)

    if not result['status']:
        logger.exception(result['message'])
        return result
    logger.info('Upi Ref Check of data Successful')
    df = result['df']

    logger.info('Starting Imps Ref Check')
    result = imps_ref_check(df)
    if not result['status']:
        logger.exception(result['message'])
        return result
    logger.info('Imps Ref Check Successful')

    df = result['df']

    logger.info('Starting Time based Checking')
    result = time_based_checking(df)
    if not result['status']:
        logger.exception(result['message'])
        return result
    logger.info('Time Based Check Successful')

    df = result['df']

    logger.info('Starting Time Check DBS')
    result = time_check_dbs(df)
    if not result['status']:
        logger.exception(result['message'])
        return result
    logger.info('Time Based Check DBS Successful')

    df = result['df']

    logger.info('Finding Monthly Credit Sum')
    result = monthly_credit_sum(df)
    if not result['status']:
        logger.exception(result['message'])
        return result
    logger.info('Monthly credit sum successful')
    r = {'status': True, 'message': 'success'}
    credit = result['r']

    logger.info('Finding Monthly Debit Sum')
    result = monthly_debit_sum(df)
    if not result['status']:
        logger.exception(result['message'])
        return result
    logger.info('Monthly debit sum successful')
    debit = result['r']
    df_result = convert_json_balanced_sheet(df, debit=debit, credit=credit)
    max_timestamp = str(df['timestamp'][df.shape[0] - 1])

    if new:
        bs_res = json.dumps(df_result)
        bs_res = json.loads(bs_res)
        bs_res['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
        bs_res['cust_id'] = user_id
        bs_res['max_timestamp']=max_timestamp
        try:
            client.analysis.balance_sheet.update({'cust_id': user_id}, {"$set": bs_res}, upsert=True)
            logger.info('balanced sheet found and saved')
            return {'status':True,'message':'success'}
        except BaseException as e:
            return {'status':False,'message':str(e)}
    else:
        logger.info("Old User updation balance_sheet")
        old_credit = old_balance_sheet['credit'][-1]
        old_debit = old_balance_sheet['debit'][-1]
        len_credit = len(old_balance_sheet['debit'])
        try:
            client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                "$push": {"sheet":{"$each": df_result['sheet']}}})
            if df_result['credit'][0][0] in old_credit[0]:
                client.analysis.balance_sheet.update_one({"cust_id": int(user_id)}, {
                    "$set": {
                        'credit.' + str(len_credit - 1) + ".1": old_credit[1] +
                                                                df_result['credit'][0][1],
                        'debit.' + str(len_credit - 1) + ".1": old_debit[1] +
                                                                df_result['debit'][0][1]
                    }
                }, upsert=True)
                client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                    "$push": {"credit":{"$each": df_result['credit'][1:]}}})
                client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                    "$push": {"debit": {"$each":df_result['debit'][1:]}}})
            else:
                client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                    "$push": {"credit": {"$each":df_result['credit']}}}),

                client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                    "$push": {"debit": {"$each":df_result['debit']}}})

            logger.info("balanced sheet sms of old user updated successfully")
            client.analysis.balance_sheet.update_one({"cust_id": int(user_id)}, {
                "$set": {"timestamp": max_timestamp,'final_credit': df_result['final_credit'],
                            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                                        upsert=True)
            return {"status":True,"message":"success"}
        except BaseException as e:
            logger.critical(f'error in balanced sheet data upload as {e}')
            return {"status":False,"message":str(e)}
