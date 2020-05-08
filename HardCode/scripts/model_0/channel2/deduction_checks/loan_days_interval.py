from HardCode.scripts.model_0.parameters.deduction_parameters.loan_limit.loan_info import loan_limit
import pandas as pd

def due_days_interval(user_id):
    count_same_app = 0
    count_diff_app = 0
    kredit = ['KREDTB', 'KRDITB','KRBEEE']
    same_app_count_check = False
    diff_app_count_check = False
    total_loans = 0


    max_limit,due_days,no_of_loan_apps,loan_apps, overdue_ratio, loan_dates, total_loans = loan_limit(user_id)
    dates = pd.DataFrame(loan_dates)

    if not dates.empty:
        dates['disbursal_date'] = pd.to_datetime(dates['disbursal_date'])
        dates = dates.sort_values(by = 'disbursal_date').reset_index(drop = True)
        #dates['disbursal_date'] = dates['disbursal_date'].astype(str)
        # total_loans = dates.shape[0]
        if dates.shape[0] > 1:
            for i in range(1,dates.shape[0]):
                diff = dates['disbursal_date'][i] - dates['disbursal_date'][i-1]
                if diff.days < 8:
                    if dates['app'][i] == dates['app'][i-1]:
                        count_same_app +=1
                        same_app_count_check = True

                    elif dates['app'][i] in kredit and dates['app'][i-1] in kredit:
                        count_same_app += 1
                        same_app_count_check = True
                    else:
                        count_diff_app +=1
                        diff_app_count_check = True

    variables = {
        'same_app_count_check' : same_app_count_check,
        'diff_app_count_check' : diff_app_count_check


    }
    values = {
        'same_app_count':count_same_app,
        'different_app_count':count_diff_app,
        'total_loans' : total_loans

    }

    return variables,values


