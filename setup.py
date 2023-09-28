from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='KaommatiPara-data-analysis',
    author="Igor",
    author_email="ihor.alfieiev@capgemini.com",
    version='1.0',
    description='A program for for combining client data with Bitcoin transactions data',
    packages=find_packages(exclude=['tests']),
    install_requires=requirements
    )
