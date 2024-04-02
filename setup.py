from setuptools import setup, find_packages

setup(
    name='hbase-health-check',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'requests',
        'click',
        'boto3',
        'orjson',
        'openpyxl',
        'dacite'
    ],
    entry_points={
        'console_scripts': [
            'hbhc=hbase_health_check.cli:cli',
        ],
    },
)
