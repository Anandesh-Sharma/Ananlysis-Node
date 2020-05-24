from HardCode.scripts.parameters_for_bl0.profile_info import get_profile_info
from datetime import datetime, date



def get_age(user_id):
    """
    :returns age of the user
    :rtype: str
    """
    age = 0

    try:
        dob, app_data, total_loans, allowed_limit, expected_date, repayment_date, reference_number, reference_relation, no_of_contacts = get_profile_info(
            user_id)
        if dob:
            dob = datetime.strptime(dob, "%Y-%m-%d")
            today = date.today()
            age = today.year - dob.year

        else:
            age = 0
    except BaseException as e:
        pass
        # print(f"Error in fetching data from api : {e}")
    finally:
        return age
