
def max_due_days(cibil_df):
    """
    :returns maximum delay(in days) made over all the loans
    :rtype: int
    """
    due_days = 0
    if cibil_df['data'] is None:  # ==>> this check is added cause in case cibil file is not uploaded
        if cibil_df['data'].empty:  # ==> dataframe is returned as None instead of an empty df
            due_days = (cibil_df['data']['Days_Past_Due'].astype(int)).max()

    return due_days
