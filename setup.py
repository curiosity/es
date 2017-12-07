from setuptools import setup, find_packages

version = "0.9.33"
requirements = ['yunobuiltin>=0.11.0',
                'elasticsearch>=1.0.0', ]


def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='es',
      version=version,
      description='Python function-based wrapper for elasticsearch-py',
      long_description=readme(),
      author='Brandon Adams',
      author_email='emidln@gmail.com',
      license='MIT/Expat',
      url='http://pypi.tools.curiositysearch.com/packages/es/',
      packages=find_packages(),
      install_requires=requirements,
      )
