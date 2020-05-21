from HardCode.scripts.parameters_for_bl0.profile_info import get_profile_info
from datetime import datetime, date
from HardCode.scripts.Util import conn
import pytz


def get_age(user_id):
    """
    :returns age of the user
    :rtype: str
    """
    age = 0
    connect = conn()
    db  = connect.analysis.parameters
    parameters = {}
    try:
        dob,app_data,total_loans,allowed_limit,expected_date,repayment_date,reference_number,reference_relation,no_of_contacts = get_profile_info(user_id)
        if dob:
            dob = datetime.strptime(dob, "%Y-%m-%d")
            today = date.today()
            age = today.year - dob.year
            parameters['age'] = age
            status = True
            msg = 'success'
        else:
            status = True
            msg = 'success'
            age = 0
    except BaseException as e:
        pass
        status = False
        msg = str(e)
        # print(f"Error in fetching data from api : {e}")
    finally:

        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                   'parameters.age': age}}, upsert=True)


        return {'status':status,'message':msg}
