from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from HardCode.scripts.model_0.rejection_criteria.relative_verification import relatives as rel
from HardCode.scripts.model_0.rejection_criteria.reference_verification.validation.cosine_similarity_method import \
    get_similarity
import pandas as pd


def rel_cos_sim(**kwargs):
    contacts = kwargs.get('contacts')

    Rel_name = []
    for key in contacts.keys():
        for contact_name in contacts[key]:
            contact_name = contact_name.lower()
            Rel_name.append(contact_name)

    similarity = []
    for i in tqdm(Rel_name):
        for j in rel.relatives:
            sim = get_similarity([i, j])
            if sim >= 0.8:
                similarity.append(sim)
            if len(similarity) >= 3:
                break

    return similarity
