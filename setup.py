from setuptools import find_packages, setup


setup(
    name='livy-submit',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'livy-submit = livy_submit.main:cli'

        ]
    }
)