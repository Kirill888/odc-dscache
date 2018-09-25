import click
from aws_utils.s3tools import make_s3_client, s3_find


@click.command('s3-ls')
@click.argument('uri', type=str, nargs=1)
@click.argument('match', type=str, default='*', nargs=1)
def cli(uri, match):
    """ List files on S3 bucket.

    Example:
       s3-ls s3://mybucket/some/path/ '*yaml'
    """

    s3 = make_s3_client()
    for url in s3_find(uri, match, s3=s3):
        print(url, flush=True)


if __name__ == '__main__':
    cli()
