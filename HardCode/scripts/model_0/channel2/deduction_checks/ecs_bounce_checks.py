from HardCode.scripts.model_0.parameters.deduction_parameters.ecs_bounce.ecs_bounce import get_count_ecs
from HardCode.scripts.model_0.parameters.deduction_parameters.ecs_bounce.chq_bounce import get_count_cb

def ecs_chq_count(user_id):
    
    count1 , status1 = get_count_ecs(user_id)
    count2 , status2 = get_count_cb(user_id)

    ecs_check = False
    cb_check = False

    if status1:
        if count1 >= 4 :
            ecs_check = True
    if status2:
        if count2 >= 2:
            cb_check = True






    variables = {
        'ecs_check' :ecs_check,
        'cb_check' : cb_check


    }

    values = {
        'ecs_bounce' :count1,
        "cheque_bounce": count2
    }

    return variables,values