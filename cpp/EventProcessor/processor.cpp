#include <pybind11/pybind11.h>

// simple function to test import
int add(int a, int b) {
    return a + b;
}

// Python module definition
PYBIND11_MODULE(_processor, m) {
    m.doc() = "Test C++ extension for EventPipeline";
    m.def("add", &add, "A function that adds two numbers");
}
