import sys

if sys.version_info < (3, 8):
    print("Error: tulona does not support this version of Python.")
    print("Please upgrade to Python 3.8 or higher.")
    sys.exit(1)

from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: tulona requires setuptools v40.1.0 or higher.")
    print("Please upgrade setuptools with 'pip install --upgrade setuptools' and try again")
    sys.exit(1)

package_name = "tulona"
package_version = "0.1.0"
description = """Database comparison is easier than ever with tulona."""

setup(
    name=package_name,
    version=package_version,
    author="mrinal",
    packages=find_namespace_packages(include=["tulona", "tulona.*"]),
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "tulona = tulona.cli.base:cli",
        ],
    },
    install_requires=[
        "wheel",
        "click~=8.1",
        # "dask[complete]~=2023.8.1",
        # "python-box[all]~=7.1",
        "ruamel.yaml~=0.18",
        # "SQLAlchemy~=2.0",
        "psycopg2-binary~=2.9",
        "pymysql~=1.1",
        "cryptography~=42.0",
        "pytest~=8.1",
        "snowflake-sqlalchemy~=1.5",
        "pyodbc~=5.1",
        "XlsxWriter~=3.2",
    ],
    python_requires=">=3.8",
)
