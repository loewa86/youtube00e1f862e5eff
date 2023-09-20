from setuptools import find_packages, setup

setup(
    name="youtube00e1f862e5eff",
    version="0.0.7",
    packages=find_packages(),
    install_requires=[
        "lxml",
        "exorde_data",
        "aiohttp",
        "yake==0.4.8",
        "keybert==0.7.0",
        "nltk==3.8.1",
        "dateparser>=1.1.3",
        "requests>=2.27.1",
        "HTMLParser"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
