from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(name='waxe.search',
      version=version,
      description="",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Aur\xc3\xa9lien Matouillot',
      author_email='a.matouillot@gmail.com',
      url='',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
          'elasticsearch',
      ],
      test_suite='nose.collector',
      setup_requires=['nose'],
      tests_require=[
        'nose',
        'nose-cov',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
