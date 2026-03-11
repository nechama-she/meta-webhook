from setuptools import setup, find_packages

setup(
    name="meta-webhook",
    version="0.1.0",
    packages=find_packages(where="src/libs"),
    package_dir={"": "src/libs"},
    install_requires=[
        "boto3>=1.26.0",
    ],
)