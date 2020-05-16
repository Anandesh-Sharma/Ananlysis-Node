from HardCode.scripts.parameters_for_bl0.relative_verification.rel_similarity import rel_sim
from HardCode.scripts.parameters_for_bl0.reference_verification.data_extraction.data import get_contacts_data
from datetime import datetime, date
from HardCode.scripts.Util import conn
import pytz


def rel_validate(user_id):
    status = True
    contacts_data = get_contacts_data(user_id)
    validated = False
    msg = ''
    connect = conn()
    db  = connect.analysis.parameters
    parameters = {}
    rel_len = 0
    try:
        if contacts_data:
            rel_status,rel_len = rel_sim(contacts=contacts_data)
            if rel_status:
                validated = True
            msg = 'validation successful'
        else:
            status = False
            msg = 'no data fetched from api'
        res = {'verification': validated, 'message': msg}
        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.relatives': res, 'parameters.no_of_relatives': rel_len}},
                  upsert=True)
        return {'status': True, 'message': msg}
    except BaseException as e:
        #print(f"Error in validation: {e}")
        msg = f"error in relatives verification : {str(e)}"


        res = {'verification': validated, 'message': msg}
        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.relatives': res,'parameters.no_of_relatives':rel_len}}, upsert=True)
        return {'status': False, 'message': msg}


