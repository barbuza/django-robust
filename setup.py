from setuptools import setup

setup(
    name='django-robust',
    version='0.1',
    description='robust background queue for django',
    author='Victor Kotseruba',
    author_email='barbuzaster@gmail.com',
    url='https://github.com/barbuza/django-robust',
    packages=['robust', 'robust.management.commands', 'robust.migrations'],
    install_requires=[
        'django > 1.9',
        'psycopg2 > 2'
    ]
)
