from HardCode.scripts.model_0.channel2.deduction_checks.active_close_checks import active_close_check
from HardCode.scripts.model_0.channel2.deduction_checks.age_checks import age_check
from HardCode.scripts.model_0.channel2.deduction_checks.loan_app_percent_check import loan_app_count_check
from HardCode.scripts.model_0.channel2.deduction_checks.payment_rating_checks import payment_rating_check
from HardCode.scripts.model_0.channel2.deduction_checks.reference_checks import reference_check
from HardCode.scripts.model_0.channel2.deduction_checks.secured_unsecured_checks import secured_unsecured_check
from HardCode.scripts.model_0.channel2.deduction_checks.loan_checks import loan_check
from HardCode.scripts.model_0.channel2.deduction_checks.ecs_bounce_checks import ecs_chq_count
from HardCode.scripts.model_0.channel2.deduction_checks.available_balance_checks import available_balance_check
from HardCode.scripts.model_0.channel2.deduction_checks.loan_days_interval import due_days_interval

def get_deduction_parameters(user_id):
    """
    :returns dictionaries of rejected parameters and their values
    :rtype: dict
    """

    bal_var , bal_val = available_balance_check(user_id)
    active_close_var, active_close_val = active_close_check(user_id)
    age_var, age_val = age_check(user_id)
    loan_app_var , loan_app_val = loan_app_count_check(user_id)
    pay_r_var ,pay_r_val = payment_rating_check(user_id)
    reference_var ,reference_val = reference_check(user_id)
    secured_unsecured_var , secured_unsecured_val = secured_unsecured_check(user_id)
    loan_var , loan_val = loan_check(user_id)
    ecs_var , ecs_val = ecs_chq_count(user_id)
    dd_var,dd_val = due_days_interval(user_id)



    rejection_variables = {
        'loan_app_count_var': loan_app_var,
        'available_balance_var': bal_var,
        'active_close_var' : active_close_var,
        'age_of_oldest_trade_var' : age_var,
        'payment_rating_var': pay_r_var,
        'reference_var': reference_var,
        'secured_unsecured_var': secured_unsecured_var,
        'loan_var': loan_var,
        'ecs_var': ecs_var
    }

    rejection_values = {
        'loan_app_count_val': loan_app_val,
        'available_balance_val': bal_val,
        'active_close_val': active_close_val,
        'age_of_oldest_trade_val': age_val,
        'payment_rating_val': pay_r_val,
        'reference_val': reference_val,
        'secured_unsecured_val': secured_unsecured_val,
        'loan_val':loan_val,
        'ecs_val': ecs_val,
        'due_days_interval_val':dd_val

    }

    return rejection_variables, rejection_values
