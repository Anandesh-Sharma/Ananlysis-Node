from HardCode.scripts.Util import conn


def fetch_user(user_id):
    # -> CHECK IF THE USER_ID IS PROCESSED BY CHECKING ANALYSIS RESULT
    client = conn()
    user_id = int(user_id)
    alys_result = client.analysis.parameters.find_one({'cust_id': user_id})

    # -> FETCH ANALYSIS
    if alys_result:
        # alys_result["result"] = [alys_result["result"][-1]]
        # del alys_result["_id"]
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
        # -> cheque bounce messages
        alys_cb = client.analysis.cheque_bounce_msg.find_one({'cust_id':user_id})
        if alys_cb:
            del alys_cb['_id']
        # -> ecs bounce messages
        alys_ecs = client.analysis.ecs_msg.find_one({'cust_id':user_id})
        if alys_ecs:
            del alys_ecs['_id']
        # -> legal messages
        alys_legal = client.analysis.legal_msg.find_one({'cust_id':user_id})
        if alys_legal:
            del alys_legal['_id']
        # -> parameters
        alys_result['parameters'] = [alys_result['parameters'][-1]]
        if alys_result:
            del alys_result['_id']

        final_result = {
            'status': True,
            'message': "Success",
            'analysis': {
                'model': alys_sm if alys_sm else {},
                'loan': alys_loan if alys_loan else {},
                'salary': alys_salary if alys_salary else {},
                'rejection': alys_rejection if alys_rejection else {},
                'balance_sheet': alys_bs if alys_bs else {},
                'cheque_bounce': alys_cb if alys_cb else {},
                'ecs_bounce': alys_ecs if alys_ecs else {},
                'legal': alys_legal if alys_legal else {},
                'parameters': alys_result if alys_result else {},

            },
        }
        return final_result
    else:
        return {'status': False, 'message': "Calm down! We're working on it"}


def pre_rejection(user_id):
    client = conn()
    user_id = int(user_id)
    rej_result = client.analysis.scoring_model.find_one({'cust_id': user_id})
    if rej_result:
        if len(rej_result['result'][-1]['rejection_reasons']) == 0:
            return {'status': True, 'rejection_status': False, 'message': " no rejection reasons found"}
        else:
            return {'status': True, 'rejection_status': True, 'message': "rejection reasons found"}
    else:
        return {'status': False, 'message': "Calm down! We're working on it"}

