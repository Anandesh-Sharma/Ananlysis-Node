from HardCode.scripts.model_0.rejection_criteria.reference_verification.data_extraction.data import \
    get_reference_details, get_contacts_data
from HardCode.scripts.model_0.rejection_criteria.reference_verification.validation.cosine_similarity_method import \
    cos_sim
from HardCode.scripts.Util import conn


def validate(user_id):
    """
    :returns True/False if the reference of mother/father verifies from the contact list
    :rtype: dict
    """
    status = True
    reference_relation, reference_number = get_reference_details(user_id)
    contacts_data = get_contacts_data(user_id)
    validated = False
    max_similarity = -9
    msg = ''
    try:
        if reference_number and reference_relation and contacts_data:

            # ==> currently validating only when the relation is either father or mother
            if reference_relation.lower() == 'mother' or reference_relation.lower() == 'father':
                cosine_similarity = cos_sim(relation=reference_relation, ref_no=reference_number,
                                            contacts=contacts_data)

                similarity = [float(i[0]) for i in cosine_similarity]
                max_similarity = round(max(similarity), 2)
                if max_similarity >= 0.80:
                    validated = True
                msg = 'validation successful'
            else:
                status = False
        else:
            status = False
            msg = 'no data fetched from api'
    except BaseException as e:
        print(f"Error in validation: {e}")
        msg = f"error in reference verification : {str(e)}"
        status = False

    finally:
        res = {'verification': validated, 'similarity_score': max_similarity, 'message': msg}

        return {'status': status, 'result': res}
