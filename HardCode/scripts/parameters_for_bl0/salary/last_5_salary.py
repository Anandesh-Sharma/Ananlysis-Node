from HardCode.scripts.Util import conn
from datetime import datetime
import calendar

def latest_salary(user_id):
    """This code gives the salary of a user from the past 5 months.

     Parameters:
         cust_id(int)    : id of the user

     Returns:
         dict : containing-
             status(bool)        : whether code ran successfully
             message(string)     : containing success of error.
             salary(float)   : salary of the user(float).
     """
    connect = conn()
    salary = 0
    sal = connect.analysis.salary.find_one({'cust_id': user_id})
    current_month = datetime.now().strftime("%m")
    year = datetime.now().strftime("%Y")
    try:
        last_5_months = int(current_month) - 5
        month_start = calendar.month_name[last_5_months]
        key = month_start+" "+year
        months = list(sal['salary'].keys())
        if key in months:
            index = months.index(key)
            months_list = months[index:]
            months_list = months_list[::-1]
            for i in months_list:
                if sal['salary'][i]['salary'] > 0:
                    salary = sal['salary'][i]['salary']
                else:
                    salary = 0
        return {"status": True, "message": "success", "salary": salary}
    except BaseException as e:
        salary = -1
        return {"status": False, "message": str(e), "salary": salary}

