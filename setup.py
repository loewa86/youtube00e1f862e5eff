from setuptools import find_packages, setup

setup(
    name="youtube00e1f862e5eff",
    version="0.0.4",
    packages=find_packages(),
    install_requires=[
        "lxml",
        "exorde_data",
        "aiohttp",
        "dateparser",
        "HTMLParser"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
