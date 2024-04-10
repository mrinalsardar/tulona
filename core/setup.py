from setuptools import find_namespace_packages, setup

if __name__ == "__main__":
    setup(
        name="tulona",
        version="0.3.2",
        packages=find_namespace_packages(include=["tulona", "tulona.*"]),
        include_package_data=True,
        entry_points={
            "console_scripts": [
                "tulona = tulona.cli.base:cli",
            ],
        },
    )
