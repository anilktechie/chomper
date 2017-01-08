from setuptools import setup, find_packages


setup(
    name='chomper',
    version='0.0.1',
    packages=find_packages(exclude=('tests', 'tests.*')),
    url='',
    license='MIT',
    author='Sam Milledge',
    author_email='sam@sammilledge.com',
    description='',
    install_requires=[
        'six>=1.10.0'
    ]
)
