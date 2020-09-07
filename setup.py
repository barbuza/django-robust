import sys

from setuptools import find_packages, setup

if sys.version_info < (3, 6):
    raise ImportError("django-robust 0.4 only supports python3.6 and newer")

setup(
    name="django-robust",
    version="0.4.0",
    description="robust background queue for django",
    author="Victor Kotseruba",
    author_email="barbuzaster@gmail.com",
    url="https://github.com/barbuza/django-robust",
    include_package_data=True,
    packages=find_packages(exclude=["django_robust", "dummy"]),
    python_requires=">=3.6",
    install_requires=[
        "django >= 2.0",
        "psycopg2",
        "django-redis",
        "redis",
        "django-object-actions",
        "schedule",
        "pygments",
    ],
)
