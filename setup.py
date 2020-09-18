from setuptools import setup

setup(
    name='less.aws',
    url='https://github.com/pashabitz/less.aws',
    author='less',
    author_email='pavelbitz@gmail.com',
    packages=['less.aws'],
    install_requires=['boto3', 'python-jose', 'requests'],
    version='0.1.0',
    license='GPL3',
    description='AWS related packages for less',
)