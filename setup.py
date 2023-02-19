from setuptools import setup

# read the contents of your README file
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="chicagorequests",
    version="0.0.2",
    author="Forest Gregg",
    author_email="fgregg@datamade.us",
    url="https://github.com/fgregg/chicagorequests/",
    install_requires=["click", "tqdm", "scrapelib", "tabulate"],
    packages=["chicagorequests"],
    entry_points={"console_scripts": ["chicagorequests=chicagorequests:main"]},
    description="command line tool for downloading Chicago Open311 data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license_files=("LICENSE.txt",),
)
