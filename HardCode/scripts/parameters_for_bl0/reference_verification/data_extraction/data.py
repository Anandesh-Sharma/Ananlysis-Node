import requests
from collections import defaultdict

URL = 'https://admin.credicxotech.com/api/get_user_info/'


def generate_access_token():
    # ==> this function is used to generate the access token in case if it expires

    url_refresh = 'https://admin.credicxotech.com/api/token/refresh/'

    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU5MjY0MzE3MCwianRpIjoiNjYyMzI4OTQ2ZTRlNGU0NWEyNjQ2ZDdlOGQzODYwOWUiLCJ1c2VyX2lkIjoxNTcwMTEsImN1cnJlbnRfc3RlcCI6MTAxLCJkZXNpZ25hdGlvbiI6WyJNTF9NYW5hZ2VyIiwiMTciXSwibmFtZSI6IlRlc3QiLCJlbWFpbCI6ImFuYW5kZXNoc2hhcm1hQGdtYWlsLmNvbSIsInBob25lX251bWJlciI6OTk5Njk0NDk0M30.Q0G3C33JUA9m2Fwm-PN7dcHincc8WRhd0zzhtnRL7pA'}

    res = requests.post(url=url_refresh, data=refresh_token)
    r = res.json()

    return r['access']



def get_contacts_data(user_id):
    # ==> fetches the user contact list

    Auth = 'Bearer ' + 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNTkyMzg5NDkwLCJqdGkiOiIwYzNiN2NlZTg4OTA0Mjc3OGRlYTJjZWI0YjNiYzg4OCIsInVzZXJfaWQiOjE1NzAxMSwiY3VycmVudF9zdGVwIjotMSwiZGVzaWduYXRpb24iOlsiTUxfTWFuYWdlciIsIjE3Il0sIm5hbWUiOiJUZXN0IiwiZW1haWwiOiJhbmFuZGVzaHNoYXJtYUBnbWFpbC5jb20iLCJwaG9uZV9udW1iZXIiOjk5OTY5NDQ5NDN9.nPkj5NqX7bBFI5mELySa3gor5vTI9Em3No5AW4ygrZQ'
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
        #print(f"Error in fetching contacts list : {e}")
        data_contacts = None

    finally:
        return data_contacts
