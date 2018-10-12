import yaml
import json
import click

from dscache.tools import slurp_lines
from dea.aws import s3_fetch, make_s3_client, s3_find


PRODUCT_MAP = dict(
    L5ARD='ls5_ard',
    L7ARD='ls7_ard',
    L8ARD='ls8_ard',
)


def parse_yaml(data):
    try:
        return yaml.load(data, Loader=yaml.CSafeLoader), None
    except Exception as e:
        return None, str(e)


def process_doc(url, data):
    metadata, error = parse_yaml(data)
    if metadata is None:
        return None, error

    p_type = metadata.get('product_type', '--')
    product = PRODUCT_MAP.get(p_type, p_type)

    out = dict(metadata=metadata,
               uris=[url],
               product=product)
    try:
        out = json.dumps(out, separators=(',', ':'), check_circular=False)
    except Exception as e:
        return None, str(e)

    return out, None


def grab_s3_yamls(input_fname, output_fname, region_name=None):
    s3 = make_s3_client(region_name=region_name)

    if input_fname.startswith('s3://'):
        bb = input_fname.split('**/')
        if len(bb) > 1:
            base, match = bb
        else:
            base, match = input_fname, None

        if not match:
            match = '*yaml'

        urls = s3_find(base, match)
        n_total = None
    else:
        urls = slurp_lines(input_fname)
        n_total = len(urls)

    with open(output_fname, 'wt') as f:
        with click.progressbar(urls, length=n_total, label='Loading from S3') as urls:
            for url in urls:
                try:
                    data = s3_fetch(url, s3)
                except:
                    print('Failed to fetch %s' % url)
                    continue

                out, error = process_doc(url, data)
                if out is None:
                    print('Failed: %s\n%s' % (url, error))
                else:
                    f.write(out)
                    f.write('\n')


@click.command('fetch-s3-yamls')
@click.argument('in_file', type=str, nargs=1)
@click.argument('out_file', type=str, nargs=1)
def cli(in_file, out_file):
    """ Turn s3 urls pointing to YAML files to JSON strings.

    \b
    For every line in `in_file`
       - Treat line as a URI and fetch YAML document from it
       - Generate JSON object with fields:
         - metadata -- contents of the YAML (parsed into object tree)
         - uris     -- list containing single uri from which `metadata` was fetched
         - product  -- product name derived from `product_type` field if present
       - Serialise JSON object to a single line in `out_file`

    \b
    Instead of file with urls you can supply a url in a form:
      `s3://bucket/some/prefix/**/*yaml`

    In this case every file under `s3://bucket/some/prefix/` that ends on `yaml` will be processed.
    """
    grab_s3_yamls(in_file, out_file)


if __name__ == '__main__':
    cli()
