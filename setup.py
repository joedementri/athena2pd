from setuptools import setup, find_packages

def find_version(path):
    import re

    # path shall be a plain ascii tetxt file
    s = open(path, 'rt').read()
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", s, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError('Version not found')

def get_requirements(filename):
    with open(filename, 'r') as fh:
        return [l.strip() for l in fh]

def get_long_desc(filename):
    with open(filename, 'r') as fh:
        return fh.read()

setup(
    name='athena2pd',
    packages=['athena2pd'],
    version=find_version('athena2pd/__init__.py'),
    description='Simple hello world as a package',
    long_description=get_long_desc('README.md'),
    long_description_content_type='text/markdown',
    author='Joe Dementri',
    author_email='joedementri42012@gmail.com',
    maintainer='Joe Dementri',
    maintainer_email='joedementri42012@gmail.com',
    license='MIT',
    # python_requires='>=3.6',
    # install_requires=get_requirements('requirements.txt'),
    zip_safe=False,
    url='https://github.com/joedementri',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
    ],
    python_requires='>3.6'
)