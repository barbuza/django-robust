from setuptools import setup, find_packages

setup(
    name='django-robust',
    version='0.1.1',
    description='robust background queue for django',
    author='Victor Kotseruba',
    author_email='barbuzaster@gmail.com',
    url='https://github.com/barbuza/django-robust',
    packages=find_packages(exclude=['django_robust']),
    install_requires=[
        'django >= 1.9',
        'psycopg2 >= 2.5'
    ]
)
