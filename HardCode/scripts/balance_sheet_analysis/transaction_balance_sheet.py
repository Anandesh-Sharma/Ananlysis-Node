from .transaction_analysis import process_data
from .monthly_transactions import monthly_credit_sum, monthly_debit_sum
from HardCode.scripts.balance_sheet_analysis.Validation2 import *
from HardCode.scripts.Util import conn, logger_1, convert_json_balanced_sheet, convert_json_balanced_sheet_empty


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
        print(index)
        if p:
            index = 0
        df = df.loc[index:]
        print(df.shape[0])
    if df.shape[0]==0:
        return {"upto_date":True,'status':True,'message':'success','new':False} # do something
    # doing something
    logger.info('Converting file to dataframe')
    if df.shape[0] == 0:
        r = {'status': True, 'message': 'success', 'df': convert_json_balanced_sheet_empty()}
        return r
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
    r['status'] = True
    r['message'] = 'success'
    debit = result['r']

    r['df'] = convert_json_balanced_sheet(df, debit=debit, credit=credit)
    r['max_timestamp']=str(df['timestamp'][df.shape[0]-1])
    r['new']=new
    r['upto_date']=False
    if not new:
        r['old_credit']=old_balance_sheet['df']['credit'][-1]
        r['old_debit']=old_balance_sheet['df']['debit'][-1]
        r['len_credit']=len(old_balance_sheet['df']['debit'])
    return r
