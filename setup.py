import setuptools

setuptools.setup(
    name='SQLExecutor',
    version='0.0.1',
    install_requires=[
        'oracledb==2.4.1',
        'configparser==7.1.0',
        'psycopg[binary]==3.2.1',
        'logging==0.4.9.6',
        'openpyxl==3.1.5'
    ],
    packages=setuptools.find_packages(include=['Executor', 'Executor.*'])
)
