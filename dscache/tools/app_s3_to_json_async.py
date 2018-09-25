import yaml
import json
import click
from collections import namedtuple
import queue

from aws_utils.s3async import fetch_bunch
from threading import Thread


Data = namedtuple('Data', 'url data idx time'.split(' '))


def parse_yaml(data):
    try:
        return yaml.load(data, Loader=yaml.CSafeLoader), None
    except Exception as e:
        return None, str(e)


def process_doc(url, data):
    metadata, error = parse_yaml(data)
    if metadata is None:
        return None, error

    out = dict(metadata=metadata,
               uris=[url])
    try:
        out = json.dumps(out, separators=(',', ':'), check_circular=False)
    except Exception as e:
        return None, str(e)

    return out, None


def d2json(d):
    from sys import stderr

    out, err = process_doc(d.url, d.data)
    if err is not None:
        print('Failed: %s\n%s' % (d.url, err), file=stderr)
    return out


def qmap(proc, q, eos_marker=None):
    while True:
        item = q.get(block=True)
        if item is eos_marker:
            break
        else:
            yield proc(item)


def q2q_map(proc, q_in, q_out, eos_marker=None):
    while True:
        item = q_in.get(block=True)
        if item is eos_marker:
            q_out.put(item, block=True)
            break
        else:
            q_out.put(proc(item))


def read_stdin_lines():
    from sys import stdin

    for l in stdin:
        l = l.strip()
        if len(l) > 0:
            yield l


@click.command('s3-to-json-async')
def cli():
    """ Turn s3 urls pointing to YAML files to JSON strings.

    \b
    For every non-empty line in stdin
       - Treat line as a URI and fetch YAML document from it
       - Generate JSON object with fields:
         - metadata -- contents of the YAML (parsed into object tree)
         - uris     -- list containing single uri from which `metadata` was fetched
         - product  -- product name derived from `product_type` field if present
       - Serialise JSON object to a single line in `out_file`
    """
    from sys import stderr

    q_raw = queue.Queue(maxsize=10_000)
    q_json = queue.Queue(maxsize=10_000)

    EOS = object()

    def on_data(data, url, idx=None, time=None):
        q_raw.put(Data(url, data, idx, time))

    def read_stage():
        fetch_bunch(read_stdin_lines(), on_data)

        for _ in range(n_worker_threads):
            q_raw.put(EOS)

    def dump_to_stdout(lines):
        for i, l in enumerate(lines):
            print(l, flush=((i % 10) == 0))
            if (i % 100) == 0:
                print('{:9,d}'.format(i), file=stderr, end='\r', flush=True)

    n_worker_threads = 4
    threads = []
    for _ in range(n_worker_threads):
        thread = Thread(target=lambda: q2q_map(d2json, q_raw, q_json, eos_marker=EOS))
        thread.start()
        threads.append(thread)

    read_thread = Thread(target=read_stage)
    read_thread.start()
    threads.append(read_thread)

    dump_to_stdout(qmap(lambda x: x, q_json, eos_marker=EOS))

    for th in threads:
        th.join()


if __name__ == '__main__':
    cli()
