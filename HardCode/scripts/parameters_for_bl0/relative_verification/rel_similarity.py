from HardCode.scripts.parameters_for_bl0.relative_verification import relatives as rel


def rel_sim(contacts):
    """ Parameters: csv object
        Output: status(bool),relatives count (int),Relatives names (list)
    """
    Rel_name = []
    contacts = contacts.split('\r\n')
    print(contacts)
    for contact in contacts:
        if len(contact) >= 0:
            splitted_list = contact.split(',')
            if len(splitted_list) == 2:
                name, _ = splitted_list

            elif len(splitted_list) == 3:
                name = splitted_list[0]
            contact_name = name
            contact_name = str(contact_name)
            res = contact_name.lower()
            res = str(res).split(' ')
            for x in res:
                for y in rel.relatives:
                    if x == y:
                        Rel_name.append(contact_name)

    return len(Rel_name)