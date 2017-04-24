#!/usr/bin/env python
import aioflow
from setuptools import setup


setup(name='aioflow',
      version=aioflow.__version__,
      description='A simple workflow implementation using asyncio.',
      author='lux.r.ck',
      author_email='lux.r.ck@gmail.com',
      packages=['aioflow'],
      install_requires=[
        "redis",
        ],
      include_package_data=True,
      license="MIT License",
      zip_safe=False,
     )
