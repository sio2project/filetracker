from os import path
import io
from setuptools import setup, find_packages

with io.open(path.join(path.abspath(path.dirname(__file__)), 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name = 'filetracker',
    version = '2.1.3',
    author = 'SIO2 Project Team',
    author_email = 'sio2@sio2project.mimuw.edu.pl',
    description = 'Filetracker caching file storage',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url = 'https://github.com/sio2project/filetracker',
    license = 'GPL',

    packages = find_packages(),

    install_requires = [
        'bsddb3==6.2.7',
        'flup6',
        'gunicorn==19.8.1',
        'gevent==1.3.1',
        'progressbar2',
        'requests',
        'six',
    ],

    setup_requires = [
        'pytest-runner',
    ],

    tests_require = [
        'pytest',
    ],

    entry_points = {
        'console_scripts': [
            'filetracker = filetracker.client.shell:main',
            'filetracker-server = filetracker.servers.run:main',
            'filetracker-cache-cleaner = filetracker.scripts.cachecleaner:main',
            'filetracker-migrate = filetracker.scripts.migrate:main',
            'filetracker-recover = filetracker.scripts.recover:main',
        ],
    }
)

