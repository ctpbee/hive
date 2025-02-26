from setuptools import setup

pkgs = ["hived", "hived.src"]

install_requires = ["ctpbee", "redis", "click", "pandas"]
setup(
    name="hived",
    version="0.1",
    description="Auto data service with ctpbee for linux. Do not support windows in here",
    author="somewheve",
    author_email="somewheve@gmail.com",
    url="https://github.com/ctpbee/hive",
    license="Apache2",
    packages=pkgs,
    install_requires=install_requires,
    platforms=["Linux", "Windows", "MacOS"],
    package_dir={"hived": "hived"},
    zip_safe=False,
    include_package_data=True,
    ext_modules=[],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    entry_points={"console_scripts": ["hived = hived:run_command"]},
)
