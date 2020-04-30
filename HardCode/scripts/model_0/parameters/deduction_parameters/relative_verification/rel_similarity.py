from HardCode.scripts.model_0.parameters.deduction_parameters.relative_verification import relatives as rel


def rel_sim(**kwargs):
    contacts = kwargs.get('contacts')
    status = False
    Rel_name = []
    for key in contacts.keys():
        for contact_name in contacts[key]:
            contact_name = str(contact_name)
            res = contact_name.lower()
            res = str(res).split(' ')
            for x in res:
                for y in rel.relatives:
                    if x == y:
                        Rel_name.append(contact_name)
                        if len(Rel_name) >= 3:
                            status = True

    return status,len(Rel_name)
