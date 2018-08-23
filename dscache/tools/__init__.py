"""
"""
import random
from .. import train_dictionary


def dictionary_from_product_list(dc,
                                 products,
                                 samples_per_product=10,
                                 dict_sz=8*1024):

    if isinstance(products, str):
        products = [products]

    limit = samples_per_product*10

    samples = []
    for p in products:
        dss = dc.find_datasets(product=p, limit=limit)
        random.shuffle(dss)
        samples.extend(dss[:samples_per_product])

    return train_dictionary(samples, dict_sz)
