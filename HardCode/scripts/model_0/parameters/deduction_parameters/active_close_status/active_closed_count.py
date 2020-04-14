from HardCode.scripts.model_0.parameters.deduction_parameters.active_close_status.account_type import closed , active

def get_active_closed(cibil_df):
    """
    :returns true if account status matches with anyone of the categories
             mentioned in the acc_status, otherwise returns false
    :rtype: bool
    """

    count_closed = 0
    count_active = 0
    status = False
    if cibil_df['data'] is not None:
        if not cibil_df['data'].empty:
            if cibil_df['data'].shape[0] >= 5:
                status = True
                status_data = cibil_df['data']['account_status']
                for st in status_data:
                    for a in closed.keys():
                        if str(st) == a:
                            count_closed += 1
                for st in status_data:
                    for b in active.keys():
                        if str(st) == b:
                            count_active += 1


    return count_closed, count_active , status

