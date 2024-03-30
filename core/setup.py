from setuptools import setup, find_namespace_packages

if __name__ == "__main__":
    setup(
        packages=find_namespace_packages(include=["tulona", "tulona.*"]),
        include_package_data=True,
        entry_points={
            "console_scripts": [
                "tulona = tulona.cli.base:cli",
            ],
        },
    )