from setuptools import find_packages
import NLP_ikaue

def define_args():
    install_requirements = ['beautifulsoup4>=4.9.1','wheel>=0.36.2','googlesearch-python>=2020.0.2',
                            'google-cloud-language>=2.0.0','pandas>=1.2.4','pytest>=6.2.3']

    test_requirements = ['pytest>=4.3,<4.4', 'pytest-cov>=2.7,<2.8']
    args = {
        'name': 'NLP_ikaue Python3 IKAUE',
        'version': NLP_ikaue.__version__,
        'url': 'https://github.com/albertlleo/python3_ikaue',
        'author': 'Albert Lleo',
        'author_email': 'albert@ikaue.com',
        'classifiers': ['Development Status :: 3 - Alpha', 'Programming Language :: Python :: 3'],
        'packages':  find_packages(),
        'python_requires': '>=3.5',
        'entry_points': {'console_scripts': ['']},
        'install_requires': install_requirements,
        'extras_require': {'testing': test_requirements},
        'project_urls': {'Source':
                         'https://github.com/albertlleo/python3_ikaue'}

    }

    return args
