from scripts.parameters_for_bl1.reference_verification.data_extraction.data import get_contacts_data
from scripts.parameters_for_bl1.profile_info import get_profile_info
from scripts.parameters_for_bl1.reference_verification.validation.cosine_similarity_method import \
    cos_sim


def validate(user_id):
    """
    :returns True/False if the reference of mother/father verifies from the contact list
    :rtype: dict
    """
    status = True
    age,app_data,total_loans,allowed_limit,expected_date,repayment_date,reference_number,reference_relation = get_profile_info(user_id)
    contacts_data = get_contacts_data(user_id)
    validated = False
    max_similarity = -9
    msg = ''


    try:
        if reference_number and reference_relation and contacts_data:

            # ==> currently validating only when the relation is either father or mother
            if reference_relation.lower() == 'mother' or reference_relation.lower() == 'father':
                cosine_similarity = cos_sim(relation=reference_relation, ref_no=str(reference_number),
                                            contacts=contacts_data)

                similarity = [float(i[0]) for i in cosine_similarity]
                if len(similarity) != 0:   # ==> this check is added to handle the case in which the contact number
                    max_similarity = round(max(similarity), 2)  # is not present in the contact list
                    if max_similarity >= 0.80:
                        validated = True
                    msg = 'validation successful'
                else:
                    msg = 'given contact number is not present in contact list'
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
