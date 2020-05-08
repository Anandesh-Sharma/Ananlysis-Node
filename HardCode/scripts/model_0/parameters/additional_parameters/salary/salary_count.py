from HardCode.scripts.Util import conn

def salary(id):
    """

    :param : id
    :return: dictionary of salary and keyword
    rtype: dict
    """
    connect = conn()
    sal = connect.analysis.salary.find_one({'cust_id':id})
    li = {}
    if sal:
        for i in range(1,len(list(sal['salary'].keys()))):
            month = list(sal['salary'].keys())[-i]
            salary = sal['salary'][month]['salary']
            keyword = sal['salary'][month]['keyword']
            time = sal['salary'][month]['message']['timestamp']
            if salary != 0:
                li = {'salary':salary,'keyword':keyword,'timestamp':time}
                break
    return li
