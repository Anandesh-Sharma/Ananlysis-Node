from HardCode.scripts.model_0.parameters.additional_parameters.credit_card_limit.cc_limit import get_cc_limit
from HardCode.scripts.model_0.parameters.additional_parameters.salary.salary_count import salary
from HardCode.scripts.model_0.parameters.deduction_parameters.loan_limit.loan_info import loan_limit
from HardCode.scripts.model_0.parameters.additional_parameters.user_name_msg.name_count_ratio import get_name_count
from HardCode.scripts.model_0.parameters.additional_parameters.age_of_user.user_age import get_age
from HardCode.scripts.model_0.parameters.deduction_parameters.rejection_msgs.get_ratio import *



def get_additional_parameters(user_id,sms_count):
    """
    :returns dictionaries of approval parameters and their values
    :rtype: dict
    """

    cc_limit = get_cc_limit(user_id)
    salary_dict = salary(user_id)
    name_count = get_name_count(user_id)
    max_limit, due_days, no_of_loan_apps, loan_apps ,loan_overdue_ratio, loan_dates = loan_limit(user_id)
    age = get_age(user_id)
    overdue_msg_ratio,overdue_msg_count = overdue_count_ratio(user_id,sms_count)
    legal_msg_ratio,legal_msg_count = legal_messages_count_ratio(user_id,sms_count)

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
    cc_list = []

    for i in cc_limit.keys():
        cc_list.append(cc_limit[i])

    if cc_list:
        c_limit = max(cc_list)

        if c_limit >= 50000:
            cc_limit_check1 = True
        if 50000 > c_limit >= 40000:
            cc_limit_check2 = True
        if 40000 > c_limit >= 30000:
            cc_limit_check3 = True
        if 30000 > c_limit >= 20000:
            cc_limit_check4 = True
        if c_limit == 0:
            cc_limit_check5 = True


     #>>==>> premium apps
    apps = ['MNYTAP','SALARY','NIRAFN','PAYSNS','PAYMEI','SUBHLN','VIVIFI','QUBERA','IAVAIL']
    premium_apps_check = False
    list_premium = []
    for ap in loan_apps:
        for aps in apps:
            if str(ap) == aps:
                list_premium.append(ap)
                premium_apps_check = True

    # >>==>> user name msg count
    # non_defaulter_check = False
    # if not defaulter:
    #     non_defaulter_check = True



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
        'salary_check5': salary_check5,
        'premium_check': premium_apps_check


    }

    approval_values = {
        'credit_card_limit': cc_limit,
        'salary': salary_dict,
        'premium_apps': list_premium,
        'name_msg_count': name_count,
        'loan_dates' : loan_dates,
        "age" : age,
        'overdue_msg_ratio' : overdue_msg_ratio,
        'overdue_msg_count' : overdue_msg_count,
        'legal_msg_ratio' : legal_msg_ratio,
        'legal_msg_count' : legal_msg_count


    }

    return approval_variables, approval_values
