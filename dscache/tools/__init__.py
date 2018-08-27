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


def db_connect(cfg=None):
    from datacube.config import LocalConfig
    import psycopg2

    if isinstance(cfg, str) or cfg is None:
        cfg = LocalConfig.find(env=cfg)

    cfg_remap = dict(dbname='db_database',
                     user='db_username',
                     password='db_password',
                     host='db_hostname',
                     port='db_port')

    pg_cfg = {k: cfg.get(cfg_name, None)
              for k, cfg_name in cfg_remap.items()}

    return psycopg2.connect(**pg_cfg)


def mk_raw2ds(products):
    from datacube.model import Dataset

    def raw2ds(ds):
        product = products.get(ds['product'], None)
        if product is None:
            raise ValueError('Missing product {}'.format(ds['product']))
        return Dataset(product, ds['metadata'], uris=ds['uris'])
    return raw2ds


def raw_dataset_stream(product, db, read_chunk=100, limit=None):
    assert isinstance(limit, (int, type(None)))

    if isinstance(db, str) or db is None:
        db = db_connect(db)

    query = '''
select
jsonb_build_object(
  'product', %(product)s,
  'uris', array((select _loc_.uri_scheme ||':'||_loc_.uri_body
                 from agdc.dataset_location as _loc_
                 where _loc_.dataset_ref = agdc.dataset.id and _loc_.archived is null
                 order by _loc_.added desc, _loc_.id desc)),
  'metadata', metadata) as dataset
from agdc.dataset
where archived is null
and dataset_type_ref = (select id from agdc.dataset_type where name = %(product)s)
{limit};
'''.format(limit='LIMIT {:d}'.format(limit) if limit else '')

    cur = db.cursor(name='c{:04X}'.format(random.randint(0, 0xFFFF)))
    cur.execute(query, dict(product=product))

    while True:
        chunk = cur.fetchmany(read_chunk)
        if not chunk:
            break

        for (ds,) in chunk:
            yield ds

    cur.close()
