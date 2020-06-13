from HardCode.scripts.parameters_for_bl0.relative_verification.rel_similarity import rel_sim
from HardCode.scripts.Util import conn


def rel_validate(user_id, contacts):
    """
        Parameters: user id.
        Output: dictionary
                status(bool),
                message(string),
                result(dictionary) keys(length, relatives names)
    """

    status = True
    validated = False
    msg = ''
    relatives = {
        'Length': 0,
        'Names': []
    }
    try:
        if contacts:
            rel_status, rel_len, rel_name = rel_sim(contacts)
            relatives['Length'] = rel_len
            relatives['Names'] = rel_name
            if rel_status:
                validated = True
            msg = 'validation successful'
        else:
            status = False
            msg = 'no data fetched from api'
        client = conn()
        client.analysis.parameters.update({'cust_id': user_id}, {"$set": relatives}, upsert=True)
        client.close()
    except BaseException as e:
        print(f"Error in validation: {e}")
        msg = f"error in relatives verification : {str(e)}"
        status = False

    finally:
        # res = {'verification': validated, 'message': msg}
        return {'status': status, 'result': relatives, 'verification': validated, "message": msg}

# rel_validate(8035)
