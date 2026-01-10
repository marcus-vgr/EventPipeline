#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

#include <map>
#include <cmath>
#include <vector>

namespace py = pybind11;

std::map<std::string, py::array_t<float>> compute_newvars(py::array_t<float> _x, py::array_t<float> _y, py::array_t<float> _energy, float epsilon){
    auto x = _x.unchecked<1>();
    auto y = _y.unchecked<1>();
    auto energy = _energy.unchecked<1>();
    const int nrows = x.shape(0);

    py::array_t<float> radius(nrows);
    py::array_t<float> momentum_proxy(nrows); 
    
    auto _radius = radius.mutable_unchecked<1>();
    auto _momentum_proxy = momentum_proxy.mutable_unchecked<1>();

    for(int i = 0; i < nrows; i++){
        _radius(i) = std::sqrt(  x(i)*x(i) + y(i)*y(i) );
        _momentum_proxy(i) = energy(i) / (_radius(i) + epsilon);
    }

    // Gotta add time_residual!

    return {{"radius", radius}, {"momentum_proxy", momentum_proxy}};

}

// Gotta add filter


// Python module definition
PYBIND11_MODULE(_processor, m) {
    m.doc() = "C++ extension for EventProcessor";
    m.def("compute_newvars", &compute_newvars, "A function compute new variables for the dataframe");
}
