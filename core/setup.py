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
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)

package_name = "tulona"
package_version = "0.1.0"
description = """Database comparison is easier than ever with tulona."""

setup(
    name=package_name,
    version=package_version,
    author="mrinal",
    author_email="mrinal.k.sardar@gmail.com",
    packages=find_namespace_packages(include=["tulona", "tulona.*"]),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'tulona = tulona.cli.base:cli',
        ],
    },
    install_requires=[
        "click~=8.1.7",
        "dask[complete]~=2023.8.1",
        "python-box[all]~=7.0",
        "ruamel.yaml~=0.17.32",
        "SQLAlchemy~=2.0.20",
        "psycopg2-binary~=2.9.7",
        "pymysql~=1.1.0",
        "cryptography~=41.0.3",
        "pytest~=7.4.0",
    ],
    python_requires=">=3.8",
)