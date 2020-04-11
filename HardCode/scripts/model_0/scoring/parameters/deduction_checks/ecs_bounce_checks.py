from HardCode.scripts.model_0.rejection_criteria.ecs_bounce.ecs_bounce import get_count_ecs

def ecs_count(user_id):

    count = get_count_ecs(user_id)

    ecs_check1 = False
    ecs_check2 = False
    ecs_check3 = False

    if count ==2 :
        ecs_check1 = True
    if count ==3 :
        ecs_check2 = True
    if count >=4 :
        ecs_check3 = True

    variables = {
        'ecs_check1' :ecs_check1,
        'ecs_check2': ecs_check2,
        'ecs_check3': ecs_check3
    }

    values = {
        'ecs_count' :count
    }

    return variables,values