from HardCode.scripts.reference_verification.data_extraction.data import get_reference_details, get_contacts_data
from HardCode.scripts.reference_verification.validation.cosine_similarity_method import cos_sim
from HardCode.scripts.Util import conn
# from pprint import pprint


def validate(user_id):
    status = True
    reference_relation, reference_number = get_reference_details(user_id)
    contacts_data = get_contacts_data(user_id)
    validated = False
    max_similarity = -9
    msg = ''
    try:
        if reference_number and reference_relation and contacts_data:
            # pprint(contacts_data)
            # print(reference_number,reference_relation)
            # ==> currently validating only when the relation is either father or mother

            if reference_relation.lower() == 'mother' or reference_relation.lower() == 'father':
                cosine_similarity = cos_sim(relation=reference_relation, ref_no=reference_number,
                                            contacts=contacts_data)

                similarity = [float(i[0]) for i in cosine_similarity]
                # pprint(similarity)
                max_similarity = max(similarity)
                if max_similarity > 0.80:
                    validated = True
                msg = 'validation successful'
        else:
            status = False
            msg = 'no data fetched from api'
    except BaseException as e:
        print(f"Error in validation: {e}")
        msg = f"error in reference verification : {str(e)}"
        status = False

    finally:
        res = {'verification': validated, 'similarity_score': max_similarity, 'message': msg}
        client = conn()
        db = client.analysis.verification
        db.update({'cust_id': user_id}, {'$set': res}, upsert=True)
        return {'cust_id': user_id, 'status': status, 'result': res}
