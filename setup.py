import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    description = "run tests"

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = None

    def finalize_options(self):
        pass

    def run(self):
        import pytest, os
        from wzdat.util import gen_dummydata
        os.environ['WZDAT_NO_CACHE'] = 'True'
        os.environ['WZDAT_DATA_DIR'] = '/wzdat/tests/dummydata'
        os.environ['WZDAT_SOL_DIR'] = '/wzdat/tests'
        os.environ['WZDAT_SOL_PKG'] = 'ws_mysol'
        os.environ['WZDAT_PRJ'] = 'myprj'
        gen_dummydata()
        errorno = pytest.main('tests')
        sys.exit(errorno)


setup(name="wzdat",
      version="0.1",
      description="Webzen Data Toolkit",
      url="http://github.com/haje01/wzdat",
      author="JeongJu Kim",
      author_email="haje01@gmail.com",
      license="MIT",
      packages=["wzdat", "wzdat.dashboard"],
      include_package_data=True,
      zip_safe=False,
      test_require=['pytest'],
      cmdclass = {'test': PyTest},
      )
