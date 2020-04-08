from HardCode.scripts.model_0.approval_criteria.active_close_status.account_type import closed , active

def get_active_closed(cibil_df):
    """
    :returns true if account status matches with anyone of the categories
             mentioned in the acc_status, otherwise returns false
    :rtype: bool
    """

    count_closed = 0
    count_active = 0
    if cibil_df['data'] is not None:
        if not cibil_df['data'].empty:
            if cibil_df['data'].shape[0] >= 5:
                status = cibil_df['data']['account_status']
                for st in status:
                    for a in closed.keys():
                        if str(st) == a:
                            count_closed += 1
                for st in status:
                    for b in active.keys():
                        if str(st) == b:
                            count_active += 1

    print(type(count_closed))
    print(type(count_active))
    print("********************************")
    return count_closed, count_active

