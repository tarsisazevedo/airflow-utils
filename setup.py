# encoding: utf-8

from setuptools import setup, find_packages

setup(
    name="airflow_utils",
    version="0.3",
    description="collection of helpers to create airflow dags",
    author="Big Data Team at globo.com",
    author_email="tarsis@corp.globo.com",
    license='MIT',
    install_requires=[
        "airflow",
        "anyjson",
    ],
    packages=find_packages(),
)
