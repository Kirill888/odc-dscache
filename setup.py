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
                      'dea-proto[async]',
                      ],
    tests_require=['pytest'],
    extras_require=dict(),
    entry_points={
        'console_scripts': [
            'slurpy = dscache.apps.slurpy:cli',
            'dstiler = dscache.apps.dstiler:cli',
        ]
    }
)
