from HardCode.scripts.parameters_for_bl0.reference_verification.data_extraction.data import get_contacts_data
from HardCode.scripts.parameters_for_bl0.profile_info import get_profile_info
from HardCode.scripts.parameters_for_bl0.reference_verification.validation.cosine_similarity_method import \
    cos_sim
from datetime import datetime, date
from HardCode.scripts.Util import conn



def validate(user_id):
    """
    :returns True/False if the reference of mother/father verifies from the contact list
    :rtype: dict
    """
    status = True
    age,app_data,total_loans,allowed_limit,expected_date,repayment_date,reference_number,reference_relation,no_of_contacts = get_profile_info(user_id)
    contacts_data = get_contacts_data(user_id)
    validated = False
    max_similarity = -9
    msg = ''
    connect = conn()
    db  = connect.analysis.parameters
    parameters = {}


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
        res = {'status': status,
               'result': {'verification': validated, 'similarity_score': max_similarity, 'message': msg}}

        return res, no_of_contacts
    except BaseException as e:
        #print(f"Error in validation: {e}")
        msg = f"error in reference verification : {str(e)}"
        res = {'status': False,
               'result': {'verification': validated, 'similarity_score': max_similarity, 'message': msg}}


        return res, no_of_contacts


