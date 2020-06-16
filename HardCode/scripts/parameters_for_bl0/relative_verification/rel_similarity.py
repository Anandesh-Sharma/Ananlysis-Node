from HardCode.scripts.parameters_for_bl0.relative_verification import relatives as rel
from fuzzywuzzy import fuzz

def rel_sim(contacts):
    """ Parameters: csv dictionary
        Output: status(bool),relatives count (int),Relatives names (list)
    """
    Rel_name = []
    for contact in contacts:
        contact_name = str(contact)
        res = str(contact_name).split(' ')
        try:
            for y in rel.relatives:
                score = fuzz.token_set_ratio(res[-1], y)
                if score >= 87:
                    Rel_name.append(contact_name)
                    raise Exception()
        except Exception:
            continue
        try:
            for x in res:
                try:
                    for y in rel.relatives:
                        score = fuzz.token_set_ratio(x, y)
                        if score >= 87:
                            Rel_name.append(contact_name)
                            raise Exception()
                except Exception:
                    raise Exception()
        except Exception:
            continue
    if len(Rel_name) > 0:
        return True, len(Rel_name), Rel_name
    else:
        return False, 0, []
