from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from HardCode.scripts.reference_verification.corpus import synonymns as syns
import pandas as pd


def cos_sim(**kwargs):
    relation = kwargs.get('relation')
    ref_no = kwargs.get('ref_no')
    contacts = kwargs.get('contacts')
    father_syns = []
    mother_syns = []

    if relation == "father":
        for word in syns.father:
            father_syns.append(word)
        for w, s in enumerate(father_syns):
            father_syns[w] = s.rstrip("\n")
    else:
        for word in syns.mother:
            mother_syns.append(word)
        for w, s in enumerate(mother_syns):
            mother_syns[w] = s.rstrip("\n")

    # ==> matching reference name in contact list

    Ref_name = []

    for key in contacts.keys():
        if int(key) == ref_no:
            for contact_name in contacts[key]:
                contact_name = contact_name.lower()
                Ref_name.append(contact_name)

    similarity = []
    if len(father_syns) != 0:
        for i in Ref_name:
            for j in father_syns:
                sim = get_similarity([i, j])
                similarity.append(sim)
    else:
        for i in Ref_name:
            for j in mother_syns:
                sim = get_similarity([i, j])
                similarity.append(sim)

    return similarity


def get_similarity(l):
    # this function computes the cosine similarity

    vectorizer = CountVectorizer().fit_transform(l)
    vec = vectorizer.toarray()
    vecx = vec[0].reshape(1, -1)
    vecy = vec[1].reshape(1, -1)
    sim = []
    sim.append(cosine_similarity(vecx, vecy))

    return sim[0][0]
