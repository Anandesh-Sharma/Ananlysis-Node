from HardCode.scripts.model_0.rejection_criteria.loan_app.loan_app_count_validate import loan_app_count

def loan_app_count_check(user_id):
    loan_app_count_percentage = loan_app_count(user_id)

    # >>==>> loan app count
    loan_app_count_check1 = False
    loan_app_count_check2 = False
    loan_app_count_check3 = False
    loan_app_count_check4 = False
    if loan_app_count_percentage >= 0.70:
        loan_app_count_check3 = True
    if 0.70 > loan_app_count_percentage >= 0.60:
        loan_app_count_check2 = True
    if 0.60 > loan_app_count_percentage >= 0.50:
        loan_app_count_check1 = True
    if loan_app_count_percentage < 0.50:
        loan_app_count_check4 = True

    variables = {
        'loan_app_count_check1': loan_app_count_check1,
        'loan_app_count_check2': loan_app_count_check2,
        'loan_app_count_check3': loan_app_count_check3,
        'loan_app_count_check4': loan_app_count_check4
    }

    values = {
        'loan_app_count' : loan_app_count_percentage
    }
    return variables,values