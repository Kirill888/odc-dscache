from uuid import UUID
import json
import lmdb
import zstandard
import operator
import functools
import itertools
import toolz
from types import SimpleNamespace
from pathlib import Path
from datacube.model import Dataset

FORMAT_VERSION = b'0001'


def key_to_bytes(k):
    if isinstance(k, str):
        return k.encode('utf8')
    if isinstance(k, UUID):
        return k.bytes
    if isinstance(k, bytes):
        return k
    if isinstance(k, int):
        if k.bit_length() < 32:
            return k.to_bytes(4, 'big')
        elif k.bit_length() < 128:
            return k.to_bytes(16, 'big')
        else:
            return str(k).decode('utf8')
    if isinstance(k, tuple):
        return functools.reduce(operator.add, map(key_to_bytes, k))

    raise ValueError('Key must be one of str|bytes|int|UUID|tuple')


def uuids2bytes(uu):
    bb = bytearray(len(uu)*16)
    for i, u in enumerate(uu):
        bb[i*16:(i+1)*16] = u.bytes
    return bytes(bb)


def bytes2uuids(bb):
    n = len(bb)//16
    return [UUID(bytes=bb[i*16:(i+1)*16]) for i in range(n)]


def prefix_visit(tr, prefix, full_key=False):
    if isinstance(prefix, str):
        prefix = prefix.encode('utf8')

    n = len(prefix)
    cursor = tr.cursor()
    cursor.set_range(prefix)
    for k, v in cursor:
        if len(k) < n or k[:n] != prefix:
            break
        yield (k if full_key else k[n:]), v


def dict2jsonKV(oo, prefix=None, compressor=None):
    for k, doc in oo.items():
        data = json.dumps(doc, separators=(',', ':')).encode('utf8')
        if compressor is not None:
            data = compressor.compress(data)
        if prefix is not None:
            k = prefix + k

        yield (key_to_bytes(k), data)


def jsonKV2dict(kv, decompressor=None):
    def decode(kv):
        k, doc = kv
        if decompressor is not None:
            doc = decompressor.decompress(doc)
        return k.decode('utf8'), json.loads(doc)

    return {k: doc for k, doc in map(decode, kv)}


def ds2bytes(ds):
    k = key_to_bytes(ds.id)

    doc = dict(uris=ds.uris,
               product=ds.type.name,
               metadata=ds.metadata_doc)

    d = json.dumps(doc, separators=(',', ':')).encode('utf8')
    return (k, d)


def doc2bytes(raw_ds):
    ''' raw_ds is

        metadata:
          id: <uuid: string>
          * other fields*
        uris: [<uri:string>]
        product: <string>
    '''
    k = UUID(toolz.get_in(['metadata', 'id'], raw_ds)).bytes
    d = json.dumps(raw_ds, separators=(',', ':')).encode('utf8')
    return (k, d)


def doc2ds(doc, products):
    p = products.get(doc['product'], None)
    if p is None:
        raise ValueError('No product named: %s' % doc['product'])
    return Dataset(p, doc['metadata'], uris=doc['uris'])


def save_products(products, transaction, compressor, overwrite=False):
    def get_metadata_definitions(products):
        mm = {}
        for p in products:
            m = p.metadata_type
            if m.name not in mm:
                mm[m.name] = m.definition

        return mm

    mm = get_metadata_definitions(products.values())
    products = toolz.valmap(lambda p: p.definition, products)

    for k, d in itertools.chain(dict2jsonKV(mm, 'metadata/', compressor),
                                dict2jsonKV(products, 'product/', compressor)):
        transaction.put(k, d, overwrite=overwrite, dupdata=False)


def build_dc_product_map(metadata_json, products_json):
    from datacube.model import metadata_from_doc, DatasetType

    mm = toolz.valmap(metadata_from_doc, metadata_json)

    def mk_product(doc, name):
        mt = doc.get('metadata_type')
        if mt is None:
            raise ValueError('Missing metadata_type key in product definition')
        metadata = mm.get(mt)

        if metadata is None:
            raise ValueError('No such metadata %s for product %s' % (mt, name))

        return DatasetType(metadata, doc)

    return {k: mk_product(doc, k) for k, doc in products_json.items()}


def train_dictionary(dss, dict_sz=8*1024):
    def to_bytes(o):
        if isinstance(o, dict):
            _, d = doc2bytes(o)
        else:
            _, d = ds2bytes(o)
        return d

    sample = list(map(to_bytes, dss))
    return zstandard.train_dictionary(dict_sz, sample).as_bytes()


class DatasetCache(object):
    """
    info:
       version: 4-bytes
       zdict: pre-trained compression dictionary, optional
       product/{name}: json
       metadata/{name}: json

    udata:
       arbitrary user data (TODO)

    ds:
       uuid: compressed(json({product: str,
                              uris: [str],
                              metadata: object}))
    """
    def __init__(self, state):
        """ Don't use this directly, use create_cache or open_cache.
        """

        self._dbs = state.dbs
        self._comp = state.comp
        self._decomp = state.decomp
        self._products = state.products

    def _store_products(self):
        with self._dbs.main.begin(self._dbs.info, write=True) as tr:
            save_products(self._products, tr, self._comp)

    def sync(self):
        if not self.readonly:
            self._store_products()

    def __del__(self):
        self.sync()

    @property
    def readonly(self):
        return self._comp is None

    @property
    def products(self):
        return self._products

    def _ds2kv(self, ds):
        k, d = ds2bytes(ds)
        d = self._comp.compress(d)
        return (k, d)

    def _doc2kv(self, ds_raw):
        k, d = doc2bytes(ds_raw)
        d = self._comp.compress(d)
        return (k, d)

    def _ds_save(self, ds, transaction):
        if ds.type.name not in self._products:
            self._products[ds.type.name] = ds.type

        k, v = self._ds2kv(ds)
        transaction.put(k, v)

    def bulk_save(self, dss):
        with self._dbs.main.begin(self._dbs.ds, write=True) as tr:
            for ds in dss:
                self._ds_save(ds, tr)

    def tee(self, dss, max_transaction_size=10000):
        """Given a lazy stream of datasets persist them to disk and then pass through
        for further processing.
        :dss: stream of datasets
        :max_transaction_size int: How often to commit results to disk
        """
        have_some = True

        while have_some:
            with self._dbs.main.begin(self._dbs.ds, write=True) as tr:
                have_some = False
                for ds in itertools.islice(dss, max_transaction_size):
                    have_some = True
                    self._ds_save(ds, tr)
                    yield ds

        self.sync()

    def bulk_save_raw(self, raw_dss):
        with self._dbs.main.begin(self._dbs.ds, write=True) as tr:
            for raw_ds in raw_dss:
                k, v = self._doc2kv(raw_ds)
                tr.put(k, v)

    def put_group(self, name, uuids):
        """ Group is a named list of uuids
        """
        data = uuids2bytes(uuids)
        k = key_to_bytes(name)

        with self._dbs.main.begin(self._dbs.groups, write=True) as tr:
            tr.put(k, data)

    def _get_group_raw(self, name):
        k = key_to_bytes(name)

        with self._dbs.main.begin(self._dbs.groups, write=False) as tr:
            return tr.get(k)

    def get_group(self, name):
        """ Group is a named list of uuids
        """
        data = self._get_group_raw(key_to_bytes(name))
        return bytes2uuids(data) if data is not None else None

    def groups(self, raw=False, prefix=None):
        """Get list of tuples (group_name, group_size).

        :raw bool: Normally names are returned as strings, supplying raw=True
        would return bytes instead, this is needed if you are using group names
        that are not strings, like integers or tuples of basic types.

        :prefix str|bytes: Only report groups with name starting with prefix
        """

        assert isinstance(prefix, (str, bytes, type(None)))

        def _raw(prefix):
            with self._dbs.main.begin(self._dbs.groups, write=False, buffers=True) as tr:
                cursor = tr.cursor() if prefix is None else prefix_visit(tr, prefix, full_key=True)
                return [(bytes(k), len(d)//16) for k, d in cursor]

        if prefix is not None:
            prefix = key_to_bytes(prefix)

        nn = _raw(prefix)
        return nn if raw else [(n.decode('utf8'), c) for n, c in nn]

    def _extract_ds(self, d):
        d = self._decomp.decompress(d)
        doc = json.loads(d)
        return doc2ds(doc, self._products)

    def get(self, uuid):
        """Extract single dataset with a given uuid, or return None if not found"""
        if isinstance(uuid, str):
            uuid = UUID(uuid)

        key = key_to_bytes(uuid)

        with self._dbs.main.begin(self._dbs.ds, buffers=True) as tr:
            d = tr.get(key, None)
            if d is None:
                return None

            return self._extract_ds(d)

    def get_all(self):
        with self._dbs.main.begin(self._dbs.ds, buffers=True) as tr:
            for _, d in tr.cursor():
                yield self._extract_ds(d)

    def stream_group(self, group_name):
        uu = self._get_group_raw(group_name)
        if uu is None:
            raise ValueError('No such group: %s' % group_name)

        if len(uu) & 0xF:
            raise ValueError('Wrong data size for group %s' % group_name)

        with self._dbs.main.begin(self._dbs.ds, buffers=True) as tr:
            for i in range(0, len(uu), 16):
                key = uu[i:i+16]
                d = tr.get(key, None)
                if d is None:
                    raise ValueError('Missing dataset for %s' % (str(UUID(bytes=key))))

                yield self._extract_ds(d)

    @property
    def count(self):
        with self._dbs.main.begin(self._dbs.ds) as tr:
            return tr.stat()['entries']


def maybe_delete_db(path):
    path = Path(path)
    if not path.exists():
        return False

    if path.is_dir():
        db, lock = [path/n for n in ["data.mdb", "lock.mdb"]]
    else:
        db = path
        lock = Path(str(path)+'-lock')

    if db.exists() and lock.exists():
        db.unlink()
        lock.unlink()

        if path.is_dir():
            path.rmdir()

    return True


def _from_existing_db(db, products=None, complevel=6):
    readonly = db.flags().get('readonly')

    try:
        db_info = db.open_db(b'info', create=False)
    except lmdb.NotFoundError:
        raise ValueError('Existing database is not a ds cache')

    with db.begin(db_info, write=False) as tr:
        version = tr.get(b'version', None)
        if version is None:
            raise ValueError('Missing format version field')
        if version != FORMAT_VERSION:
            raise ValueError("Unsupported on disk version: " + version.decode('utf8'))

        zdict = tr.get(b'zdict', None)

    dbs = SimpleNamespace(main=db,
                          info=db_info,
                          groups=db.open_db(b'groups', create=False),
                          ds=db.open_db(b'ds', create=False),
                          udata=db.open_db(b'udata', create=False))

    comp_params = {'dict_data': zstandard.ZstdCompressionDict(zdict)} if zdict else {}

    comp = None if readonly else zstandard.ZstdCompressor(level=complevel, **comp_params)
    decomp = zstandard.ZstdDecompressor(**comp_params)

    if products is None:
        with db.begin(db_info, write=False) as tr:
            metadata = jsonKV2dict(prefix_visit(tr, 'metadata/'), decomp)
            products = jsonKV2dict(prefix_visit(tr, 'product/'), decomp)

        products = build_dc_product_map(metadata, products)

    state = SimpleNamespace(dbs=dbs,
                            comp=comp,
                            decomp=decomp,
                            products=products)

    return DatasetCache(state)


def _from_empty_db(db,
                   complevel=6,
                   zdict=None):
    assert isinstance(zdict, (bytes, type(None)))

    db_info = db.open_db(b'info', create=True)

    with db.begin(db_info, write=True) as tr:
        tr.put(b'version', FORMAT_VERSION)

        if zdict is not None:
            tr.put(b'zdict', zdict)

    dbs = SimpleNamespace(main=db,
                          info=db_info,
                          groups=db.open_db(b'groups', create=True),
                          ds=db.open_db(b'ds', create=True),
                          udata=db.open_db(b'udata', create=True))

    comp_params = {'dict_data': zstandard.ZstdCompressionDict(zdict)} if zdict else {}

    comp = zstandard.ZstdCompressor(level=complevel, **comp_params)
    decomp = zstandard.ZstdDecompressor(**comp_params)

    state = SimpleNamespace(dbs=dbs,
                            comp=comp,
                            decomp=decomp,
                            products={})

    return DatasetCache(state)


def open_ro(path,
            products=None,
            lock=False):
    """Open existing database in readonly mode.

    NOTE: default mode assumes db file is static (not being modified
    externally), if this is not the case, supply `lock=True` parameter.

    :path str: Path to the db could be folder or actual file

    :products: Override product dictionary with compatible products loaded from
    the datacube database, this is generally only needed if you intend to add
    datasets to the datacube index directly (i.e. without product matching
    metadata documents).

    :lock bool: Supply True if external process is changing DB concurrently.

    """

    subdir = Path(path).is_dir()

    db = lmdb.open(path,
                   subdir=subdir,
                   max_dbs=8,
                   lock=lock,
                   create=False,
                   readonly=True)

    return _from_existing_db(db, products=products)


def open_rw(path,
            products=None,
            max_db_sz=None,
            complevel=6):
    """Open existing database in append mode.

    :path str: Path to the db could be folder or actual file

    :products: Override product dictionary with compatible products loaded from
    the datacube database, this is generally only needed if you intend to add
    datasets to the datacube index directly (i.e. without product matching
    metadata documents).

    :max_db_sz int: Maximum size in bytes database file is allowed to grow to, defaults to 10Gb

    :complevel: Compression level (Zstandard) to use when storing datasets, 1
    fastest, 6 good and still fast, 20+ best but slower.
    """

    subdir = Path(path).is_dir()

    if max_db_sz is None:
        max_db_sz = 10*(1 << 30)

    db = lmdb.open(path,
                   subdir=subdir,
                   max_dbs=8,
                   map_size=max_db_sz,
                   lock=True,
                   create=False,
                   readonly=False)

    return _from_existing_db(db, products=products, complevel=complevel)


def create_cache(path,
                 complevel=6,
                 zdict=None,
                 max_db_sz=None,
                 truncate=False):
    """
    """

    if truncate:
        maybe_delete_db(path)

    if max_db_sz is None:
        max_db_sz = 10*(1 << 30)

    db = lmdb.open(path,
                   max_dbs=8,
                   map_size=max_db_sz,
                   create=True,
                   readonly=False)

    # If db is not empty just call open on it
    if db.stat()['entries'] > 0:
        return _from_existing_db(db, complevel=complevel)
    else:
        return _from_empty_db(db, complevel=complevel, zdict=zdict)


def test_key_to_value():

    for k in ("string", 217987, 215781587158712587, ("AAA", 3)):
        bb = key_to_bytes(k)
        assert isinstance(bb, bytes)

    assert key_to_bytes(UUID(bytes=b"0123456789ABCDEF")) == b"0123456789ABCDEF"
    assert key_to_bytes(b"88") == b"88"


def test_create_cache():
    ss = create_cache('tmp.lmdb', truncate=True)
    print(ss)
    del ss
    ss = open_ro('tmp.lmdb')
    print(ss)
