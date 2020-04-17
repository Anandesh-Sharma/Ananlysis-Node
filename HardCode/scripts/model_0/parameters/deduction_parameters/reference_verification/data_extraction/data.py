import requests
from collections import defaultdict

URL = 'https://admin.credicxotech.com/api/get_user_info/'
Auth = 'Bearer ' + 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4OTIyMDY3OSwianRpIjoiNTM2YzM4NWU2NzNjNDg1ODhjZGEwY2UzMDA4NTA2NzgiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.e9_9BKfyehBBYzwJQ9uvmmrcAh2GR0JzCYSED-hhLKQ'


def generate_access_token():
    # ==> this function is used to generate the access token in case if it expires

    url_refresh = 'https://admin.credicxotech.com/api/token/refresh/'

    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4OTIyMDY3OSwianRpIjoiNTM2YzM4NWU2NzNjNDg1ODhjZGEwY2UzMDA4NTA2NzgiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.e9_9BKfyehBBYzwJQ9uvmmrcAh2GR0JzCYSED-hhLKQ'}

    res = requests.post(url=url_refresh, data=refresh_token)
    r = res.json()

    return r['access']


def get_reference_details(user_id):
    # ==> fetches reference relation and number from the api

    global Auth
    reference_relation, reference_number = None, None

    param = {'user_id': user_id}
    try:
        res = requests.get(url=URL, params=param, headers={'Authorization': Auth})

        if res.status_code == 404:
            raise BaseException

        if res.status_code == 401:
            access_token = generate_access_token()
            Auth = 'Bearer ' + access_token
            res = requests.get(url=URL, params=param, headers={'Authorization': Auth})
            if res.status_code == 404:
                raise BaseException
            else:
                data = res.json()
                if 'error' not in data:
                    if data['profile']['preference__relation'] and data['profile']['preference_number']:
                        reference_relation = data['profile']['preference__relation'].lower()
                        reference_number = data['profile']['preference_number']
        else:
            data = res.json()
            if 'error' not in data:
                if data['profile']['preference__relation'] and data['profile']['preference_number']:
                    reference_relation = data['profile']['preference__relation'].lower()
                    reference_number = data['profile']['preference_number']

    except BaseException as e:
        print(f"Error in fetching data from api : {e}")
    finally:
        return reference_relation, reference_number


def get_contacts_data(user_id):
    # ==> fetches the user contact list

    global Auth
    data_contacts = defaultdict(list)
    url_contacts = 'https://admin.credicxotech.com/UserInfo/' + str(user_id) + '/contacts.csv'

    try:
        contacts_data = requests.get(url=url_contacts, headers={'Authorization': Auth})
        if contacts_data.status_code == 404:
            # print("contacts does not exist")
            raise BaseException

        if contacts_data.status_code == 401:

            access_token = generate_access_token()
            Auth = 'Bearer ' + access_token
            contacts_data = requests.get(url=url_contacts, headers={'Authorization': Auth})
            if contacts_data.status_code == 404:
                # print("contacts does not exist does not exist")
                raise BaseException

            else:
                contacts = contacts_data.text

        else:
            contacts = contacts_data.text

        contacts = contacts.split('\r\n')
        for contact in contacts:
            if len(contact) >= 0:

                splitted_list = contact.split(',')
                if len(splitted_list) == 2:
                    name, number = splitted_list
                    data_contacts[number].append(name)

                elif len(splitted_list) == 3:
                    name = splitted_list[0]
                    number = splitted_list[2]
                    data_contacts[number].append(name)

    except BaseException as e:
        print(f"Error in fetching contacts list : {e}")
        data_contacts = None

    finally:
        return data_contacts