from setuptools import setup, find_packages

setup(
    name='django-robust',
    version='0.1.15',
    description='robust background queue for django',
    author='Victor Kotseruba',
    author_email='barbuzaster@gmail.com',
    url='https://github.com/barbuza/django-robust',
    include_package_data=True,
    packages=find_packages(exclude=['django_robust', 'dummy']),
    install_requires=[
        'django >= 1.9,<=2.2.9',
        'psycopg2-binary>=2.7.4',
        'django-object-actions',
        'schedule',
        'colorlog',
        'pygments'
    ]
)
