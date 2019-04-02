from setuptools import setup

setup(
    name='wts',
    version='1.0.0',
    description='A quantitative trading system',
    author='Xutao Chen',
    author_email='xc1139@nyu.edu',
    packages=['wts'],
    install_requires=['pycodestyle', 'sqlalchemy', 'tushare', 'bs4', 'pandas',
                      'numpy'],
    # external packages as dependencies
)
