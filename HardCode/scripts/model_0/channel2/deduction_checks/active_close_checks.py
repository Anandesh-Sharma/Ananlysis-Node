from HardCode.scripts.parameters_for_bl0.active_close_status.active_closed_count import get_active_closed
from HardCode.scripts.Util import conn

def active_close_check(user_id):
    connect = conn()
    parameters = connect.analysis.parameters.find_one({'cust_id':user_id})['parameters'][-1]
    # active_count, closed_count , status = get_active_closed(cibil_df)
    active_count = parameters['active']
    closed_count = parameters['closed']
    # >>==>> active closed account
    active_close_check1 = False
    active_close_check2 = False
    active_close_check3 = False
    active_close_check4 = False
    active_close_check5 = False
    active_close_check = False
    account = (active_count + closed_count)

    if active_count <= account * 0.33:
        active_close_check1 = True
    if account * 0.33 < active_count <= account * 0.50:
        active_close_check2 = True
    if account * 0.50 < active_count <= account * 0.70:
        active_close_check3 = True
    if account * 0.70 < active_count <= account * 0.90:
        active_close_check4 = True
    if active_count >= account * 0.90:
        active_close_check5 = True

    else:
        active_close_check = True
    connect.close()
    variables = {
        'active_close_check1': active_close_check1,
        'active_close_check2': active_close_check2,
        'active_close_check3': active_close_check3,
        'active_close_check4': active_close_check4,
        'active_close_check5': active_close_check5,
        'active_close_check': active_close_check
    }
    values = {
        'active_count': active_count,
        'closed_count': closed_count,
    }

    return variables,values
