# from HardCode.scripts.model_0.parameters.deduction_parameters.ecs_bounce.ecs_bounce import get_count_ecs
# from HardCode.scripts.model_0.parameters.deduction_parameters.ecs_bounce.chq_bounce import get_count_cb
from HardCode.scripts.Util import conn


def ecs_chq_count(user_id):
    connect = conn()
    parameters = connect.analysis.parameters.find_one({"cust_id":user_id})['parameters'][-1]
    count1 = parameters['ecs_bounce']
    count2 = parameters['chq_bounce']
    # count1 = get_count_ecs(user_id)
    # count2 , status2 = get_count_cb(user_id)

    ecs_check = False
    cb_check = False


    if count1 >= 4 :
        ecs_check = True

    if count2 >= 2:
        cb_check = True
    connect.close()
    variables = {
        'ecs_check' :ecs_check,
        'cb_check' : cb_check

    }

    values = {
        'ecs_bounce' :count1,
        "cheque_bounce": count2
    }

    return variables,values