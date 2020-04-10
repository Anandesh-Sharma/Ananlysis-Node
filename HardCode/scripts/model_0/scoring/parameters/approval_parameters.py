from HardCode.scripts.model_0.approval_criteria.credit_card_limit.cc_limit import get_cc_limit
from HardCode.scripts.model_0.approval_criteria.salary.salary_count import salary



def get_approval_parameters(user_id):
    """
    :returns dictionaries of approval parameters and their values
    :rtype: dict
    """

    cc_limit = get_cc_limit(user_id)
    salary_dict = salary(user_id)


    # >>==>> salary
    salary_check1 = False
    salary_check2 = False
    salary_check3 = False
    salary_check4 = False
    salary_check5 = False
    if salary_dict:
        if salary_dict['keyword'] == "epf" or salary_dict['keyword'] == "salary":
            if salary_dict['salary'] >= 25000:
                salary_check1 = True
            elif 25000 > salary_dict['salary'] >= 20000:
                salary_check2 = True
            elif 20000 > salary_dict['salary'] >= 15000:
                salary_check3 = True
            elif 15000 > salary_dict['salary'] >= 10000:
                salary_check4 = True
            elif 10000 > salary_dict['salary']:
                salary_check5 = True


    # >>==>> credit card limit
    cc_limit_check1 = False
    cc_limit_check2 = False
    cc_limit_check3 = False
    cc_limit_check4 = False
    cc_limit_check5 = False
    for i in cc_limit.keys():
        if cc_limit[i] >= 50000:
            cc_limit_check1 = True
        elif 50000 > cc_limit[i] >= 40000:
            cc_limit_check2 = True
        elif 40000 > cc_limit[i] >= 30000:
            cc_limit_check3 = True
        elif 30000 > cc_limit[i] >= 20000:
            cc_limit_check4 = True
        elif cc_limit[i] == 0:
            cc_limit_check5 = True





    approval_variables = {
        'cc_limit_check1': cc_limit_check1,
        'cc_limit_check2': cc_limit_check2,
        'cc_limit_check3': cc_limit_check3,
        'cc_limit_check4': cc_limit_check4,
        'cc_limit_check5': cc_limit_check5,
        'salary_check1': salary_check1,
        'salary_check2': salary_check2,
        'salary_check3': salary_check3,
        'salary_check4': salary_check4,
        'salary_check5': salary_check5


    }

    approval_values = {
        'credit_card_limit': cc_limit,
        'salary': salary_dict

    }

    return approval_variables, approval_values
