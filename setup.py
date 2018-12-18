from setuptools import find_packages, setup
import versioneer

setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    name="livy-submit",
    packages=find_packages(),
    entry_points={"console_scripts": ["livy = livy_submit.cli:cli"]},
)
