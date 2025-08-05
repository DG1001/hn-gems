from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="hn-hidden-gems",
    version="0.1.0",
    author="HN Hidden Gems",
    author_email="contact@hnhiddengems.com",
    description="Discover high-quality Hacker News posts from low-karma accounts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DG1001/hn-gems",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.9",
    install_requires=[
        "Flask>=3.1.1",
        "python-dotenv>=1.1.1",
        "requests>=2.32.4",
        "SQLAlchemy>=2.0.0",
        "Flask-SQLAlchemy>=3.0.0",
        "algoliasearch>=3.0.0",
        "openai>=1.0.0",
        "celery>=5.3.0",
        "redis>=5.0.0",
        "python-dateutil>=2.8.0",
        "beautifulsoup4>=4.12.0",
        "feedgen>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "hn-gems=hn_hidden_gems.cli:main",
        ],
    },
)