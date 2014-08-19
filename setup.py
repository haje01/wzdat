from setuptools import setup

setup(name="wzdat",
      version="0.1",
      description="Webzen Data Toolkit",
      url="http://github.com/haje01/wzdat",
      author="JeongJu Kim",
      author_email="haje01@gmail.com",
      license="MIT",
      packages=["wzdat", "wzdat.dashboard"],
      include_package_data=True,
      zip_safe=False)
