from HardCode.scripts.model_0.parameters.scoring_criteria import rel_cos_sim
from HardCode.scripts.model_0.parameters.scoring_criteria import get_contacts_data


def rel_validate(user_id):
    status = True
    contacts_data = get_contacts_data(user_id)
    validated = False
    msg = ''
    try:
        if contacts_data:
            rel_cosine_similarity = rel_cos_sim(contacts=contacts_data)
            similarity = [float(i[0]) for i in rel_cosine_similarity]
            if len(similarity) >= 3:
                validated = True
            msg = 'validation successful'
        else:
            status = False
            msg = 'no data fetched from api'
    except BaseException as e:
        print(f"Error in validation: {e}")
        msg = f"error in relatives verification : {str(e)}"
        status = False

    finally:
        res = {'verification': validated, 'message': msg}
        return {'status': status, 'result': res}

# rel_validate(8035)
