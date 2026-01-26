# lhn/setup.py
from setuptools import setup, find_packages

setup(
    name='lhn',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[],  # No dependencies enforced by pip
    author='Harlan A Nelson',
    author_email='hnelson3@iuhealth.org',
    description='HealthEIntent data processing utilities',
)