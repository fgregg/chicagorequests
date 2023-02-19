from setuptools import setup

setup(
    name="chicagorequests",
    version="0.0.1",
    install_requires=["click", "tqdm", "scrapelib", "tabulate"],
    packages=["chicagorequests"],
    entry_points={"console_scripts": ["chicagorequests=chicagorequests:main"]},
)
