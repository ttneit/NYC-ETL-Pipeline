from setuptools import find_packages, setup

setup(
    name="NYC_Open_Data",
    packages=find_packages(exclude=["NYC_Open_Data_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
