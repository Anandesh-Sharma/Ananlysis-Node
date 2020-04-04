from HardCode.scripts.model_0.rejection_criteria.account_status.account_types import acc_types


def get_acc_status(cibil_df):
    """
    :returns true if account type matches with anyone of the categories
             mentioned in the account_types, otherwise returns false
    :rtype: bool
    """
    account_status = True
    if cibil_df['data'] is not None:  # ==>> this check is added cause in case cibil file is not uploaded
        if cibil_df['data'].empty:  # ==> dataframe is returned as None instead of an empty df
            account = cibil_df['data']['account_type']
            for acc in account:
                for c in acc_types.keys():
                    if str(acc) == c:
                        account_status = False
                        return account_status

    return account_status
