from setuptools import find_packages
import YANDEX_EXTRACTOR

def define_args():
    install_requirements = ['oauth2client>=4.1.3','wheel>=0.36.2','google-cloud-bigquery>=2.13.1','google-api-python-client>=2.2.0',
                            'google-cloud-language>=2.0.0','pandas>=1.2.4','pytest>=6.2.3']

    test_requirements = ['pytest>=4.3,<4.4', 'pytest-cov>=2.7,<2.8']
    args = {
        'name': 'YANDEX_EXTRACTOR Python3 IKAUE',
        'version': YANDEX_EXTRACTOR.__version__,
        'url': 'https://github.com/albertlleo/',
        'author': 'Albert Lleo',
        'author_email': 'albert@ikaue.com',
        'classifiers': ['Development Status :: 3 - Alpha', 'Programming Language :: Python :: 3'],
        'packages':  find_packages(),
        'python_requires': '>=3.5',
        'entry_points': {'console_scripts': ['']},
        'install_requires': install_requirements,
        'extras_require': {'testing': test_requirements},
        'project_urls': {'Source':
                         'https://github.com/albertlleo/'}

    }

    return args
