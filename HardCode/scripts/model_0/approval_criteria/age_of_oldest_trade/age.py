
def age_oldest_trade(cibil_df):
    """
    :param cibil_df
    :returns age of oldest trade in months
    :rtype: int
    """
    age_of_oldest_trade = 0
    if cibil_df['data'].empty:
        age_of_oldest_trade = int(cibil_df['data']['age_of_oldest_trade'].iloc[-1])
        age_of_oldest_trade = round(age_of_oldest_trade / 30)

    return age_of_oldest_trade

# 24 months n above - good
# 18 months - decent
# 1 year  - risky
