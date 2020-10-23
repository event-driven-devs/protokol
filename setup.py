import os
from setuptools import setup, find_packages


dir_path = os.path.dirname(__file__)
version_path = os.path.join(dir_path, 'VERSION')
with open(version_path) as f:
    semver = f.read().strip()
build = os.environ.get('CI_PIPELINE_ID', None)
version = '{}-{}'.format(semver, build) if build else semver

setup(
    name='protokol',
    version=version,
    description='NATS-oriented RPC and Event protocol',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.5',
        'Operating System :: MacOS',
        'Operating System :: POSIX :: Linux',
    ],
    author='Sergey Zhegunya',
    author_email='harmonicsseven@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        "asyncio-nats-client",
    ],
    extras_require={
        "test": [
            "asynctest",
            "openapi-spec-validator",
            "pytest",
            "pytest-aiohttp",
            "pytest-cov",
        ],
        "develop": [
            "bumpversion",
        ]
    },
    include_package_data=True,
    package_data={
        'protokol': [
            'VERSION',
        ]
    }
)
