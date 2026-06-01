from setuptools import setup, find_packages

setup(
    name="nexus-runtime",
    version="1.0.0",
    description="Nexus Runtime — Causally-consistent, crash-recoverable execution substrate for AI agents",
    long_description=open("../README.md", encoding="utf-8").read() if __import__("os").path.exists("../README.md") else "",
    long_description_content_type="text/markdown",
    author="Nexus Runtime Team",
    license="MIT",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=[
        "typing_extensions>=4.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
)
