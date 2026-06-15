from setuptools import setup, find_packages

setup(
    name="orcaglue_shared_lib",
    version="0.1.0",
    # Automatically finds the internal shared_lib directory
    packages=find_packages(),
    install_requires=[
        "pulumi>=3.0.0",
    ],
    author="OrcaGlue",
    description="OrcaGlue Shared utility libraries for Pulumi monorepo",
)
