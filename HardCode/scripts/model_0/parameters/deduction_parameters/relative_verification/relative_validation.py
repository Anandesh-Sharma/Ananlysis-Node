from HardCode.scripts.model_0.parameters.deduction_parameters.relative_verification.rel_similarity import rel_sim
from HardCode.scripts.model_0.parameters.deduction_parameters.reference_verification.data_extraction.data import get_contacts_data


def rel_validate(user_id):
    status = True
    contacts_data = get_contacts_data(user_id)
    validated = False
    msg = ''
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
    except BaseException as e:
        #print(f"Error in validation: {e}")
        msg = f"error in relatives verification : {str(e)}"
        status = False

    finally:
        res = {'verification': validated, 'message': msg}
        return {'status': status, 'length':rel_len,'result': res}

# rel_validate(8035)
