import requests

URL = 'https://admin.credicxotech.com/api/get_user_info/'
Auth = 'Bearer ' + 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9' \
                   '.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNTg1NDk2NzU3LCJqdGkiOiJmYjZmNjA2MDdlOTc0M2E3OTYyNmM5YjkwMTZhZGExNSIsInVzZXJfaWQiOjE3LCJjdXJyZW50X3N0ZXAiOjEsImRlc2lnbmF0aW9uIjpbInN1cGVydXNlcl9zdXBlcnVzZXIiXSwibmFtZSI6IlN1cmFqIEJvaGFyYSIsImVtYWlsIjoic3VyYWouYm9oYXJhLjU4OUBnbWFpbC5jb20ifQ.uPCZUEjd9nA-mgxS6ItUrpkE2mOKIQ_jXAVjh1YiGdw '


def generate_access_token():
    # ==> this function is used to generate the access token in case if it expires
    url_refresh = 'https://admin.credicxotech.com/api/token/refresh/'
    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9'
                   '.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4NjY4NjU0NCwianRpIjoiNDk5ZGQ4MDlmYWEzNDFlNGI0NmY4M2IwNTZkODA3OTUiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIn0.CUNH83SYpbsK6xj5xIHstj7Q8OIQyRR0m_ug6tqJWf0'}
    res = requests.post(url=url_refresh, data=refresh_token)
    r = res.json()
    return r['access']


def get_reference_details(user_id):
    # ==> fetches reference relation and number from the api
    global Auth
    profile_data = None
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
                    profile_data = data['apps']

        else:
            data = res.json()
            if 'error' not in data:
                profile_data = data['apps']
    except BaseException as e:
        print(f"Error in fetching data from api : {e}")
    finally:
        return profile_data
