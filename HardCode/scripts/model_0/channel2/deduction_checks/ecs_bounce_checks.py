from HardCode.scripts.model_0.parameters.deduction_parameters.ecs_bounce.ecs_bounce import get_count_ecs
from HardCode.scripts.model_0.parameters.deduction_parameters.ecs_bounce.chq_bounce import get_count_cb

def ecs_count(user_id):

    count1 , status1 = get_count_ecs(user_id)
    count2 , status2 = get_count_cb(user_id)

    ecs_check1 = False
    ecs_check2 = False
    ecs_check3 = False

    if status1:
        if count1 ==2 :
            ecs_check1 = True
        if count1 ==3 :
            ecs_check2 = True
        if count1 >=4 :
            ecs_check3 = True





    variables = {
        'ecs_check1' :ecs_check1,
        'ecs_check2': ecs_check2,
        'ecs_check3': ecs_check3,

    }

    values = {
        'ecs_bounce' :count1,
        "cheque_bounce": count2
    }

    return variables,values