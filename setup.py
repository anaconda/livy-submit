from setuptools import find_packages, setup
import versioneer

# with open('requirements.txt') as fp:
#     install_requires = fp.read().splitlines()

# with open('requirements-dev.txt') as fp:
# 	test_require = fp.read().splitlines()

setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    name="livy-submit",
    packages=find_packages(),
    entry_points={"console_scripts": ["livy = livy_submit.cli:cli"]},
    python_requires=">=3.6",
    # Using conda to install everything
    #install_requires=install_requires,
    #test_require=test_require
)
