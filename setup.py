from setuptools import setup, find_packages

setup(
    name='dscache',
    version='0.1',
    license='Apache License 2.0',
    packages=find_packages(),
    author='Kirill Kouzoubov',
    author_email='kirill.kouzoubov@ga.gov.au',
    description='TODO',
    python_requires='>=3.5',
    install_requires=['datacube',
                      'zstandard',
                      'lmdb',
                      'click',
                      'toolz',
                      'aiohttp',
                      ],
    tests_require=['pytest'],
    extras_require=dict(),
    entry_points={
        'console_scripts': [
            'index_from_json = dscache.tools.index_from_json:cli',
            'fetch_s3_yamls = dscache.tools.fetch_s3_to_json:cli',
            'slurpy = dscache.tools.slurpy:cli',
            'dstiler = dscache.tools.dstiler:cli',
            's3-find = dscache.tools.app_s3_find:cli',
            's3-yaml-to-json = dscache.tools.app_s3_to_json_async:cli',
        ]
    }
)
