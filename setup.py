from setuptools import setup, find_packages
import os

def requirements(filename: str):
    with open(os.path.join('requirements', filename)) as f:
        return f.readlines()

setup(
    name='caqi',
    version='1.0',
    packages=find_packages(),
    url='',
    license='MIT',
    author='Chris Gervang',
    author_email='chris@gervang.com',
    install_requires=requirements('main.txt'),
    description='Cumulative AQI Metric'
)
