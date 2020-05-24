
from HardCode.scripts.parameters_for_bl0.profile_info import get_profile_info


def loan_app_count(user_id):
    """
    :returns percentage of loan apps installed
    :rtype: float
    """
    age,app_data,total_loans,allowed_limit,expected_date,repayment_date,reference_number,reference_relation,no_of_contacts = get_profile_info(user_id)
    percentage_of_loan_apps = 0
    try:
        if app_data:
            d = []
            for i in app_data:
                # TODO >== prepare a list of loan apps and
                #          check from that instead of using finance keyword

                if i['app__category'] == 'FINANCE':
                    d.append(i)

            percentage_of_loan_apps = (len(d) / len(app_data))
        return percentage_of_loan_apps
    except BaseException as e:

        return percentage_of_loan_apps
