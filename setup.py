import sys

from setuptools import find_packages, setup

if sys.version_info < (3, 6):
    raise ImportError('django-robust 0.2.x only supports python3.6 and newer')

setup(
    name='django-robust',
    version='0.2.0',
    description='robust background queue for django',
    author='Victor Kotseruba',
    author_email='barbuzaster@gmail.com',
    url='https://github.com/barbuza/django-robust',
    include_package_data=True,
    packages=find_packages(exclude=['django_robust', 'dummy']),
    python_requires='>=3.6',
    install_requires=[
        'django >= 1.11',
        'psycopg2 >= 2.5',
        'django-redis >= 4.8.0',
        'redis >= 2.10',
        'django-object-actions',
        'schedule',
        'pygments'
    ]
)
