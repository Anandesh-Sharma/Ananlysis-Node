from HardCode.scripts.Util import conn, logger_1
from datetime import datetime
import pytz

def get_recently_open_loan_details(cust_id):
    logger = logger_1('get_recently_open_loan_details', cust_id)
    target_apps = ['KREDTB', 'CASHBN', 'LNFRNT', 'CSHMMA', 'KREDTZ', 'RRLOAN', 'FRLOAN', 'WFCASH', 'FLASHO', 'MONTAP','LOANAP', 'ABCFIN', 'KISSHT', 'FRLOAN', 'SHUBLN', 'PAYMIN']
    recent_open_count = 0
    app_list = []
    parameters = {  }
    current_date = datetime.now()
    try:
        client = conn()
        logger.info("successfully connect to the db")
        loan_info = client.analysis.loan.find_one({"cust_id" : cust_id})
        loan_info = loan_info['complete_info']

        if loan_info:
            logger.info("loan info found in db")
            for app, _ in loan_info.items():
                if app in target_apps:
                    logger.info("app found in target apps")
                    last_loan_index = list(loan_info[app].keys())[-1]
                    last_loan_info = loan_info[app][last_loan_index]
                    #disbursed_date = last_loan_info["disbursed_date"]
                    #due_date = last_loan_info["due_date"]
                    if last_loan_info["disbursed_date"] != -1:
                        date = datetime.strptime(str(last_loan_info["disbursed_date"]), "%Y-%m-%d %H:%M:%S")
                        if (current_date - date).days < 15:
                            logger.info("recently open loan found")
                            recent_open_count += 1
                            app_list.append(app)
                    elif last_loan_info["due_date"] != -1:
                        date = datetime.strptime(str(last_loan_info["due_date"]), "%Y-%m-%d %H:%M:%S")
                        if (current_date - date).days < 15:
                            logger.info("recently open loan found")
                            recent_open_count += 1
                            app_list.append(app)
                    else:
                        logger.info("not found info about recent loan")
        parameters['recent_open_count'] = recent_open_count
        parameters['recent_open_apps'] = app_list
        db = client.analysis.loan
        db.update_one({'cust_id': cust_id},
          {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                    'recent_open_details': parameters}}, upsert=True)
        print(app_list)
        print(recent_open_count)
    except BaseException as e:
        import traceback
        traceback.print_tb(e.__traceback__)
    finally:
        return recent_open_count