import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gtfs-lite", # Replace with your own username
    version="0.1",
    author="Willem Klumpenhouwer",
    author_email="willem@klumpentown.com",
    description="A lightweight pandas-driven package for analyzing static GTFS feeds.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/wklumpen/gtfs-lite",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)