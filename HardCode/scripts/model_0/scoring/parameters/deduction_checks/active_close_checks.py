from HardCode.scripts.model_0.rejection_criteria.active_close_status.active_closed_count import get_active_closed

def active_close_check(cibil_df):
    active_count, closed_count , status = get_active_closed(cibil_df)

    # >>==>> active closed account
    active_close_check1 = False
    active_close_check2 = False
    active_close_check3 = False
    active_close_check4 = False
    active_close_check5 = False
    active_close_check = False
    account = (active_count + closed_count)
    if status:
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

    if not status:
        active_close_check = True

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
