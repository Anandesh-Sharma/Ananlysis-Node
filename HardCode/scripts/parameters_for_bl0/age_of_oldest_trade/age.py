from HardCode.scripts.Util import conn
import pytz
from datetime import datetime

def age_oldest_trade(cibil_df):
    """
    :param cibil_df
    :returns age of oldest trade in months
    :rtype: int
    """
    age_of_oldest_trade = 0

    if cibil_df:
        if cibil_df['data'] is not None:    # ==>> this check is added cause in case cibil file is not uploaded
            if not cibil_df['data'].empty:      # ==> dataframe is returned as None instead of an empty df
                age_of_oldest_trade = int(cibil_df['data']['age_of_oldest_trade'].iloc[-1])
                age_of_oldest_trade = round(age_of_oldest_trade / 30)

    return age_of_oldest_trade


# 24 months n above - good
# 18 months - decent
# 1 year  - risky
