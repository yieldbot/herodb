from setuptools import setup, find_packages
setup(
    name = "herodb",
    version = "0.1",
    packages = find_packages(),
	install_requires = ['dulwich>=0.8.3', 'bottle>=0.10.9', 'restkit>=4.1.2'],
	#extra_requires = {
	#	'store':  ['dulwich>=0.8.3'],
	#	'server': ['bottle>=0.10.9'],
	#	'client': ['restkit>=4.1.2'],
	#},
	# metadata for upload to PyPI
	author = "Dave White",
	author_email = "dwhite@yieldbot.com",
	description = "A simple key/value store using git as the backing store.",
	license = "PSF",
	keywords = "git key value store database",
	url = "http://github.com/yieldbot/gitstore",   # project home page, if any
)