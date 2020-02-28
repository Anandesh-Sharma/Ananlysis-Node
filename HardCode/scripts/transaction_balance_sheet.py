from transaction_analysis import process_data
from monthly_transactions import monthly_credit_sum,monthly_debit_sum,pd
from Validation2 import *
from Util import conn, logger_1,convert_json_balanced_sheet,convert_json_balanced_sheet_empty
import json


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
        db = client.messagecluster
        file1 = db.transaction.find_one({"_id": user_id})
    except Exception as e:
        logger.exception(e)
        return {'status': False, 'message': 'Type Error:user_id not int type'}

    logger.info('conncection successful')

    if file1 is None:
        logger.error("file doesn't exist in database")
        return {'status': False, 'message': "file doesn't exist in database"}

    logger.info('Converting file to dataframe')
    df = pd.DataFrame(file1['sms'])
    if df.shape[0]==0:
        r = {'status':True,'message':'success'}
        r['df'] = convert_json_balanced_sheet_empty()
        return r
    logger.info('Conversion Successful')
    logger.info('Starting to process data')
    result = process_data(df,user_id)

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
    r={'status':True,'message':'success'}
    credit= result['r']

    logger.info('Finding Monthly Debit Sum')
    result = monthly_debit_sum(df)
    if not result['status']:
        logger.exception(result['message'])
        return result
    logger.info('Monthly debit sum successful')
    r['status'] = True
    r['message'] ='success'
    debit = result['r']
    
    r['df'] = convert_json_balanced_sheet(df,debit=debit,credit=credit)
    return r
