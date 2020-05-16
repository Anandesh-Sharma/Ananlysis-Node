from HardCode.scripts.Util import logger_1, conn
import pandas as pd

def fetch_user_data(cust_id):
    """
    This function establishes a connection from the mongo database and fetches data of the user.

    Parameters:
        cust_id(int)                : id of the user
        script_status(dictionary)   : a dictionary for reporting errors occured at various stages
    Returns:
        loan_data(dataframe)        : dataframe containing messages of loan disbursal, loan closed and due/overdue
        trans_data(dataframe)       : dataframe containing only transactional messgaes of the user
    """
    logger = logger_1('get_customer_data', cust_id)

    try:
        client = conn()
        # connect to database
        db = client.messagecluster
        logger.info("Successfully established the connection with DataBase")

        # connect to collection
        approval_data = db.loanapproval
        disbursed_data = db.disbursed
        overdue_data = db.loandueoverdue
        closed_data = db.loanclosed

        closed = closed_data.find_one({"cust_id": cust_id})
        disbursed = disbursed_data.find_one({"cust_id": cust_id})
        approval = approval_data.find_one({"cust_id": cust_id})
        overdue = overdue_data.find_one({"cust_id": cust_id})
        loan_data = pd.DataFrame(columns=['sender', 'body', 'timestamp', 'read'])
        if len(closed['sms']) != 0:
            closed_df = pd.DataFrame(closed['sms'])
            loan_data = loan_data.append(closed_df)
            logger.info("Found loan closed data")
        else:
            logger.info("loan closed data not found")

        if len(disbursed['sms']) != 0:
            disbursed_df = pd.DataFrame(disbursed['sms'])
            loan_data = loan_data.append(disbursed_df)
            logger.info("Found loan disbursed data")
        else:
            logger.info("loan disbursed data not found")

        if len(overdue['sms']) != 0:
            overdue_df = pd.DataFrame(overdue['sms'])
            loan_data = loan_data.append(overdue_df)
            logger.info("Found loan overdue data")
        else:
            logger.info("loan overdue data not found")

        loan_data.sort_values(by=["timestamp"])

        loan_data = loan_data.reset_index(drop=True)
        script_status = {'status': True, "result": loan_data}
        client.close()
    except:
        r = {'status': False, 'message': str(e),
            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
        client.analysisresult.exception_bl0.insert_one(r)
        logger.info('unable to fetch data')
        script_status = {'status': False, 'message': 'unable to fetch data'}
        client.close()
    finally:
        return loan_data
