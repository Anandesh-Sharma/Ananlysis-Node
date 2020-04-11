
def max_due_days(cibil_df):
    """
    :returns maximum delay(in days) made over all the loans
    :rtype: int
    """
    due_days = 0
    status = False
    if cibil_df['data'] is not None:  # ==>> this check is added cause in case cibil file is not uploaded
        if not cibil_df['data'].empty:  # ==> dataframe is returned as None instead of an empty df
            due_days = (cibil_df['data']['Days_Past_Due'].astype(int)).max()
            due_days = int(due_days)
            status = True

    return due_days , status
