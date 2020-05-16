from datetime import datetime, date
from HardCode.scripts.Util import conn
import pytz
from HardCode.scripts.parameters_for_bl0.profile_info import get_profile_info


def loan_app_count(user_id):
    """
    :returns percentage of loan apps installed
    :rtype: float
    """
    age,app_data,total_loans,allowed_limit,expected_date,repayment_date,reference_number,reference_relation = get_profile_info(user_id)
    percentage_of_loan_apps = 0
    connect = conn()
    db  = connect.analysis.parameters
    parameters = {}
    try:

        if app_data:
            d = []
            for i in app_data:
                # TODO >== prepare a list of loan apps and
                #          check from that instead of using finance keyword

                if i['app__category'] == 'FINANCE':
                    d.append(i)

            percentage_of_loan_apps = (len(d) / len(app_data))
            status = True
            msg = 'success'

    except BaseException as e:
        status = False

        print(f"Error in loan app count validation : {e}")

    finally:
        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.percentage_of_loan_apps': percentage_of_loan_apps}}, upsert=True)
        return {'status': status, 'message': msg}
