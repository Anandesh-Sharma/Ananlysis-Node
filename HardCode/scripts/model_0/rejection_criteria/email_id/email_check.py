from email_data import get_email_details



def email_check(id):
    count = 0
    data = get_email_details(id)
    if data:
        data = data.split("@")[0]
        for i in data:
            if i.isnumeric():
                count+=1
        if count > 4 :
            return False  # False being rejection of user
        else:
            return True
    else:
        return False

            
    
