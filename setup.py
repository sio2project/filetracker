from setuptools import setup, find_packages
setup(
    name = "filetracker",
    version = '1.1.0',
    author = "SIO2 Project Team",
    author_email = 'sio2@sio2project.mimuw.edu.pl',
    description = "Filetracker caching file storage",
    url = 'https://github.com/sio2project/filetracker',
    license = 'GPL',

    packages = find_packages(),

    install_requires = [
        'poster >= 0.7',
        'flup',
    ],

    entry_points = {
        'console_scripts': [
            'filetracker = filetracker.shell:main',
            'filetracker-server = filetracker.servers.run:main',
            'filetracker-cache-cleaner = filetracker.cachecleaner:main',
        ],
    }
)

