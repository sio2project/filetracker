from setuptools import setup, find_packages
setup(
    name = 'filetracker',
    version = '2.0',
    author = 'SIO2 Project Team',
    author_email = 'sio2@sio2project.mimuw.edu.pl',
    description = 'Filetracker caching file storage',
    url = 'https://github.com/sio2project/filetracker',
    license = 'GPL',

    packages = find_packages(),

    install_requires = [
        'bsddb3',
        'flup6',
        'gunicorn',
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
            'filetracker-cache-cleaner = filetracker.cachecleaner:main',
            'filetracker-migrate = filetracker.scripts.migrate:main',
            'filetracker-recover = filetracker.scripts.recover:main',
        ],
    }
)

