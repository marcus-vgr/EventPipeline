from setuptools import setup, Extension
import pybind11
from setuptools import find_packages

# Define the C++ extension
ext_modules = [
    Extension(
        "EventPipeline.EventProcessor._processor",                     
        ["cpp/EventPipeline/EventProcessor/processor.cpp"],     
        include_dirs=[pybind11.get_include()],
        language="c++",
    )
]

setup(
    name="EventPipeline",
    version="0.1.0",
    description="High-performance event data processing pipeline",
    packages=find_packages(where="python"), 
    package_dir={"": "python"},
    ext_modules=ext_modules,
    zip_safe=False,                              
)