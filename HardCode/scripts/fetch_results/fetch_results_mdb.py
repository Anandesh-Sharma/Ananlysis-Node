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
        alys_rejection = client.messagecluster.loanrejection.find_one({'cust_id': user_id})
        if alys_loan:
            del alys_rejection["_id"]
        # -> scoring_model
        alys_sm = client.analysis.scoring_model.find_one({'cust_id': user_id})
        alys_sm["result"] = [alys_sm["result"][-1]]
        if alys_sm:
            del alys_sm["_id"]
        # -> cheque bounce messages
        alys_cb = client.messagecluster.cheque_bounce_msgs.find_one({'cust_id': user_id})
        if alys_cb:
            del alys_cb['_id']
        # -> ecs bounce messages
        alys_ecs = client.messagecluster.ecs_msgs.find_one({'cust_id': user_id})
        if alys_ecs:
            del alys_ecs['_id']
        # -> legal messages
        alys_legal = client.messagecluster.legal_msgs.find_one({'cust_id': user_id})
        if alys_legal:
            del alys_legal['_id']
        # -> analysis result
        alys_result_bl0 = client.analysisresult.bl0.find_one({'cust_id': user_id})
        if alys_result_bl0:
            del alys_result_bl0['_id']
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
                'result':alys_result_bl0 if alys_result_bl0 else []
            },
        }
        return final_result
    else:
        return {'status': False, 'message': "Calm down! We're working on it"}


def fetch_user_messages(user_id):
    # -> CHECK IF THE USER_ID IS PROCESSED BY CHECKING ANALYSIS RESULT
    client = conn()
    user_id = int(user_id)
    try:
        # -> parameters
        mydb = client['messagecluster']
        collections = [col for col in mydb.list_collection_names()]
        coll_dict = dict()
        for col in collections:
            coll_dict[col] = mydb[col].find_one({'cust_id': user_id})
            del coll_dict[col]['_id']
            del coll_dict[col]['cust_id']
        final_result = {
            'status': True,
            'message': "Success",
            'message_cluster': mydb
        }
        client.close()
        return final_result
    except:
        client.close()
        return {'status': False, 'message': "Calm down! We're working on it"}
