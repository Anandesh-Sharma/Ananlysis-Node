from HardCode.scripts.Util import conn
import json


def fetch_user(user_id):
    # -> CHECK IF THE USER_ID IS PROCESSED BY CHECKING ANALYSIS RESULT
    client = conn()
    user_id = int(user_id)
    alys_result = client.analysisresult.bl0.find_one({'cust_id': user_id})

    # -> FETCH ANALYSIS
    if alys_result:
        alys_result["result"] = [alys_result["result"][-1]]
        del alys_result["_id"]
        # -> balance_sheet
        alys_bs = client.analysis.balance_sheet.find_one({'cust_id': user_id})
        if alys_bs:
            del alys_bs["_id"]
        # -> salary
        alys_salary = client.analysis.salary.find_one({'cust_id': user_id})
        if alys_salary:
            del alys_salary["_id"]
        # -> loan
        alys_loan = client.analysis.loan.find_one({'cust_id': user_id})
        if alys_loan:
            del alys_loan["_id"]
        # -> rejection
        alys_rejection = client.analysis.rejection.find_one({'cust_id': user_id})
        if alys_loan:
            del alys_rejection["_id"]
        # -> scoring_model
        alys_sm = client.analysis.scoring_model.find_one({'cust_id': user_id})
        alys_sm["result"] = [alys_sm["result"][-1]]
        if alys_sm:
            del alys_sm["_id"]

        final_result = {
            'status': True,
            'message': "Success",
            'analysis_result': alys_result,
            'analysis': {
                'model': alys_sm if alys_sm else {},
                'loan': alys_loan if alys_loan else {},
                'salary': alys_salary if alys_salary else {},
                'rejection': alys_rejection if alys_rejection else {},
                'balance_sheet': alys_bs if alys_bs else {},
            },
        }
        return json.dumps(final_result)
    else:
        return {'status': False, 'message': "Calm down! We're working on it"}