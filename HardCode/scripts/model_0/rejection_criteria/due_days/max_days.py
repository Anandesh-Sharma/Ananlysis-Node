
def max_due_days(cibil_df):
    """
    :returns maximum delay(in days) made over all the loans
    :rtype: int
    """
    due_days = 0
    if cibil_df:
        due_days = (cibil_df['data']['Days_Past_Due'].astype(int)).max()

    return due_days
