from HardCode.scripts.Util import logger_1, conn
import pandas as pd
from datetime import datetime
import pytz

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
        disbursed_data = db.disbursed
        overdue_data = db.loanoverdue
        due_data = db.loandue
        closed_data = db.loanclosed
        rejected_data = db.loanrejection

        closed = closed_data.find_one({"cust_id": cust_id})
        disbursed = disbursed_data.find_one({"cust_id": cust_id})
        overdue = overdue_data.find_one({"cust_id": cust_id})
        due = due_data.find_one({"cust_id": cust_id})
        rejected = rejected_data.find_one({"cust_id": cust_id})

        loan_data = pd.DataFrame(columns=['sender', 'body', 'timestamp', 'read'])
        if len(closed['sms']) != 0:
            closed_df = pd.DataFrame(closed['sms'])
            closed_df["category"] = "closed"
            loan_data = loan_data.append(closed_df)
            logger.info("Found loan closed data")
        else:
            logger.info("loan closed data not found")

        if len(disbursed['sms']) != 0:
            disbursed_df = pd.DataFrame(disbursed['sms'])
            disbursed_df["category"] = "disbursed"
            loan_data = loan_data.append(disbursed_df)
            logger.info("Found loan disbursed data")
        else:
            logger.info("loan disbursed data not found")

        if len(overdue['sms']) != 0:
            overdue_df = pd.DataFrame(overdue['sms'])
            overdue_df["category"] = "overdue"
            loan_data = loan_data.append(overdue_df)
            logger.info("Found loan overdue data")
        else:
            logger.info("loan overdue data not found")

        if len(due['sms']) != 0:
            due_df = pd.DataFrame(due['sms'])
            due_df["category"] = "due"
            loan_data = loan_data.append(due_df)
            logger.info("Found loan overdue data")
        else:
            logger.info("loan overdue data not found")

        if len(rejected['sms']) != 0:
            rejected_df = pd.DataFrame(rejected['sms'])
            rejected_df["category"] = "rejected"
            loan_data = loan_data.append(rejected_df)
            logger.info("Found loan rejection data")
        else:
            logger.info("loan rejection data not found")

        loan_data.sort_values(by=["timestamp"])

        loan_data = loan_data.reset_index(drop=True)
        script_status = {'status': True, "result": loan_data}
        client.close()
    except BaseException as e:
        r = {'status': False, 'message': str(e),
            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': cust_id}
        client.analysisresult.exception_bl0.insert_one(r)
        logger.info('unable to fetch data')
        script_status = {'status': False, 'message': 'unable to fetch data'}
        client.close()
    finally:
        return loan_data
