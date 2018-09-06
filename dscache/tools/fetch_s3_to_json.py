import yaml
import json
import click

from aws_utils import slurp_lines
from aws_utils.s3tools import s3_fetch, make_s3_client


PRODUCT_MAP = dict(
    L5ARD='ls5_ard',
    L7ARD='ls7_ard',
    L8ARD='ls8_ard',
)


def parse_yaml(data):
    return yaml.load(data, Loader=yaml.CSafeLoader)


def grab_s3_yamls(input_fname, output_fname, region_name=None):
    urls = slurp_lines(input_fname)

    n_total = len(urls)
    s3 = make_s3_client(region_name=region_name)

    with open(output_fname, 'wt') as f:
        for idx, url in enumerate(urls):
            try:
                data = s3_fetch(url, s3)
            except:
                print('Failed to fetch %s' % url)
                continue

            metadata = parse_yaml(data)
            p_type = metadata.get('product_type', '--')
            product = PRODUCT_MAP.get(p_type, p_type)

            out = dict(metadata=metadata,
                       uris=[url],
                       product=product)
            out = json.dumps(out, separators=(',', ':'), check_circular=False)

            f.write(out)
            f.write('\n')

            if (idx % 100) == 0:
                print('.', end='')

            if (idx % 1000) == 0:
                print('{:5.1f}%'.format(100*idx/n_total))


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
    """
    grab_s3_yamls(in_file, out_file)


if __name__ == '__main__':
    cli()
