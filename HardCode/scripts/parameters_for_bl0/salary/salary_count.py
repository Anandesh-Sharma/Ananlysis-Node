from HardCode.scripts.Util import conn
from datetime import datetime
import pytz
from HardCode.scripts.salary_analysis.monthly_salary_analysis import salary_main


def salary(user_id):
    """

    :param : id
    :return: list of dictionary of salary for specified months
    rtype: list of dict
    """
    connect = conn()
    salary_main(user_id)
    sal = connect.analysis.salary.find_one({'cust_id': user_id})
    dict_of_sal = []
    no_of_month = 6
    if sal:
        months = list(sal['salary'].keys())[::-1]
        month_key = months[:no_of_month]
        for i in month_key:
            dict_of_sal.append(sal['salary'][i])

    return dict_of_sal

def avg_sal(user_id):
    """

    :param : id
    :return: avg of last n months salary
    rtype: float
    """
    dict_of_sal = salary(user_id)
    list_of_sal = []
    avg_sal = 0
    if dict_of_sal:
        for i in range(len(dict_of_sal)):
            list_of_sal.append(dict_of_sal[i]['salary'])
            avg_sal = sum(list_of_sal) / len(list_of_sal)
    return avg_sal

def max_sal(user_id):
    """

    :param : id
    :return: max of salary
    rtype: float
    """
    dict_of_sal = salary(user_id)
    list_of_sal = []
    max_sal = 0
    if dict_of_sal:
        for i in range(len(dict_of_sal)):
            list_of_sal.append(dict_of_sal[i]['salary'])
            print(f"max_sal: {list_of_sal}")
            max_sal = max(list_of_sal)
    return max_sal

def last_sal(user_id):
    """

    :param : id
    :return: last salary found
    rtype: float
    """
    dict_of_sal = salary(user_id)
    list_of_sal = []
    last_sal = 0
    if dict_of_sal:
        for i in range(len(dict_of_sal)):
            list_of_sal.append(dict_of_sal[i]['salary'])
        for j in range(len(list_of_sal)):
            if list_of_sal[-j] != 0:
                last_sal = list_of_sal[-j]
                break
    return last_sal

def quarantine_sal(user_id):
    connect = conn()
    salary = connect.analysis.salary.find_one({'cust_id':user_id})
    sal = -1
    try:
        if salary:
            if "May" in list(salary['salary'].keys()):
                sal = salary['salary']['May']['salary']
            elif "April" in list(salary['salary'].keys()):
                sal = salary['salary']['April']['salary']
            else:
                sal = -1
        connect.close()
        return sal
    except:
        connect.close()
        return sal





