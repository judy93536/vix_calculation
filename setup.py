from setuptools import setup, find_packages

setup(
    name="vix_calculator",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas",
        "numpy",
        "sqlalchemy",
        "psycopg2-binary",
        "yfinance",
        "python-dotenv",
    ],
    python_requires=">=3.10",
)
