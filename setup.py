import sys

from setuptools import find_packages, setup

setup(
    name="django-robust",
    version="0.5.0",
    description="robust background queue for django",
    author="Victor Kotseruba",
    author_email="barbuzaster@gmail.com",
    url="https://github.com/barbuza/django-robust",
    include_package_data=True,
    packages=find_packages(exclude=["django_robust", "dummy"]),
    python_requires=">=3.8",
    install_requires=[
        "django >= 3.0, < 5.0",
        "psycopg2",
        "django-redis",
        "redis",
        "django-object-actions",
        "schedule",
        "pygments",
    ],
)
