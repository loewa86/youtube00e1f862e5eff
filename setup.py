from setuptools import find_packages, setup

setup(
    name="youtube00e1f862e5eff",
    version="0.0.48",
    packages=find_packages(),
    install_requires=[
        "lxml",
        "exorde_data",
        "aiohttp",
        "nltk==3.6.2",
        "dateparser>=1.1.3",
        "requests>=2.27.1",
        "HTMLParser",
        "beautifulsoup4>=4.11"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
