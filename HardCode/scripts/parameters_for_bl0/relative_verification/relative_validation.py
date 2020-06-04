from HardCode.scripts.parameters_for_bl0.relative_verification.rel_similarity import rel_sim
from HardCode.scripts.parameters_for_bl0.reference_verification.data_extraction.data import get_contacts_data



def rel_validate(user_id):

    contacts_data = get_contacts_data(user_id)
    validated = False
    rel_len = 0
    try:
        if contacts_data:
            rel_status,rel_len = rel_sim(contacts=contacts_data)
            if rel_status:
                validated = True
            msg = 'validation successful'
        else:

            msg = 'no data fetched from api'
        res = {'verification': validated, 'message': msg}

        return res, rel_len
    except BaseException as e:
        #print(f"Error in validation: {e}")
        msg = f"error in relatives verification : {str(e)}"

        res = {'verification': validated, 'message': msg}
        return res , rel_len


