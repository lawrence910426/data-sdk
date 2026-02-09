from setuptools import setup, find_packages

setup(
    name="data-sdk",
    version="0.1.0",
    packages=["data_sdk", "data_sdk.crawlers"],
    package_dir={"data_sdk": "src"},
    install_requires=[
        "pandas",
        "FinMind",
        "shioaji",
        "requests",
    ],
    author="User",
    description="Data SDK for FinMind and Shioaji wrappers and crawlers",
    python_requires=">=3.6",
)
