from setuptools import find_packages, setup

setup(
    name="dagster_etl2",
    packages=find_packages(exclude=["dagster_etl2_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
