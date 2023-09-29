from setuptools import setup, find_packages

setup(
    name='KaommatiPara-data-analysis',
    author="Igor",
    author_email="ihor.alfieiev@capgemini.com",
    version='1.0',
    license='No',
    description='A program for combining client data with Bitcoin transactions data',
    packages=find_packages(exclude=['tests']),
    install_requires='pyspark',
    url='https://github.com/CyberIgor/Simple-PySpark-app-Test'
    )
