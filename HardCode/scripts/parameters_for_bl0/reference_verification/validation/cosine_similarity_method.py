from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from scripts.parameters_for_bl1.reference_verification.corpus import synonymns as syns


def cos_sim(**kwargs):
    relation = kwargs.get('relation')
    ref_no = kwargs.get('ref_no')
    contacts = kwargs.get('contacts')
    # father_syns = []
    # mother_syns = []

    # if relation == "father":
    #     for word in syns.father:
    #         father_syns.append(word)
    #     for w, s in enumerate(father_syns):
    #         father_syns[w] = s.rstrip("\n")
    # else:
    #     for word in syns.mother:
    #         mother_syns.append(word)
    #     for w, s in enumerate(mother_syns):
    #         mother_syns[w] = s.rstrip("\n")

    # ==> matching reference name in contact list

    Ref_name = []
    for key in contacts.keys():
        if str(key[-10:]) == ref_no[-10:]:
            for contact_name in contacts[key]:
                res = ''.join([i for i in contact_name if not i.isdigit()])
                res = res.lower()
                res = str(res).split(' ')
                for i in res:
                    for j in syns.subscribers:
                        if i == j:
                            res.remove(i)
                res = " ".join(res)
                Ref_name.append(res)

    similarity = []
    if len(Ref_name) != 0:         # ==> this check is added to handle the case in which the contact number
        if relation == "father":    # is not present in the contact list
            for i in Ref_name:
                for j in syns.father:
                    sim = get_similarity([i, j])
                    similarity.append(sim)
        else:
            for i in Ref_name:
                for j in syns.mother:
                    sim = get_similarity([i, j])
                    similarity.append(sim)

    return similarity


def get_similarity(l):
    # this function computes the cosine similarity

    vectorizer = CountVectorizer().fit_transform(l)
    vec = vectorizer.toarray()
    vecx = vec[0].reshape(1, -1)
    vecy = vec[1].reshape(1, -1)
    sim = [cosine_similarity(vecx, vecy)]

    return sim[0][0]
