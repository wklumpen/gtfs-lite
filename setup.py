import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gtfs-lite",
    version="0.2.0",
    author="Willem Klumpenhouwer",
    author_email="willem@klumpentown.com",
    description="A lightweight Pandas-driven package for analyzing static GTFS feeds.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/wklumpen/gtfs-lite",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Natural Language :: English",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pandas>=1.0',
    ]
)