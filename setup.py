from setuptools import setup, find_packages


setup(
    name="concurrent_dag",
    version="0.1.0",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    python_requires=">=3.10",

    install_requires=[
        "pydantic",
        "marsh-lib"
    ],

    # entry_points={
    #     "console_scripts": [
    #         "dag = src.main:main",
    #     ]
    # },

    author="Cedric Anover",
    author_email="cedric.anover@hotmail.com",
    description="Concurrent DAG Proof-of-Concept.",
    license="MIT"
)
