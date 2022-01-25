from setuptools import setup, find_packages
from os import path as op


def _read(fname):
    try:
        return open(op.join(op.dirname(__file__), fname)).read()
    except IOError:
        return ''


install_requires = [
    l for l in _read('requirements.txt').split('\n') if l and not l.startswith('#')]

setup(name='watchtower.alert',
      version='0.1',
      description=_read('DESCRIPTION'),
      url='https://github.com/CAIDA/watchtower-alert',
      author='Alistair King',
      author_email='alistair@caida.org',
      license='UCSD-Non-Commerical-Academic',
      packages=find_packages(),
      include_package_data=True,
      entry_points={'console_scripts': [
          'watchtower-alert=watchtower.alert.consumer:main'
      ]},
      install_requires=install_requires
      )
