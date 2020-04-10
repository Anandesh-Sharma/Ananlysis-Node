

def approval_score(approval_variables):
    score = 0
    weights = {}

    cc_limit_check1 = approval_variables['cc_limit_check1']
    cc_limit_check2 = approval_variables['cc_limit_check2']
    cc_limit_check3 = approval_variables['cc_limit_check3']
    cc_limit_check4 = approval_variables['cc_limit_check4']
    cc_limit_check5 = approval_variables['cc_limit_check5']
    salary_check1 = approval_variables['salary_check1']
    salary_check2 = approval_variables['salary_check2']
    salary_check3 = approval_variables['salary_check3']
    salary_check4 = approval_variables['salary_check4']
    salary_check5 = approval_variables['salary_check5']


    if cc_limit_check1:
        score += 60
        weights['cc_limit'] = '+60'

    if cc_limit_check2:
        score += 40
        weights['cc_limit'] = '+40'

    if cc_limit_check3:
        score += 30
        weights['cc_limit'] = '+30'

    if cc_limit_check4:
        score += 20
        weights['cc_limit'] = '+20'

    if cc_limit_check5:
        score += 10
        weights['cc_limit'] = '+10'

    if salary_check1:
        score += 30
        weights['salary'] = '+30'

    if salary_check2:
        score += 25
        weights['salary'] = '+25'

    if salary_check3:
        score += 20
        weights['salary'] = '+20'

    if salary_check4:
        score += 15
        weights['salary'] = '+15'

    if salary_check5:
        score += 10
        weights['salary'] = '+10'

    return score, weights



