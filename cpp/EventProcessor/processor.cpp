#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

#include <map>
#include <cmath>
#include <vector>

namespace py = pybind11;

std::map<std::string, py::array_t<float>> compute_newvars(py::array_t<float> x, py::array_t<float> y, py::array_t<float> energy, py::array_t<float> timestamp, py::array_t<int> detector_id, float epsilon){
    auto x_view = x.unchecked<1>();
    auto y_view = y.unchecked<1>();
    auto energy_view = energy.unchecked<1>();
    auto timestamp_view = timestamp.unchecked<1>();
    auto detector_id_view = detector_id.unchecked<1>();
    const int nrows = x_view.shape(0);

    py::array_t<float> radius(nrows);
    py::array_t<float> momentum_proxy(nrows); 
    py::array_t<float> time_residual(nrows);
    auto radius_view = radius.mutable_unchecked<1>();
    auto momentum_proxy_view = momentum_proxy.mutable_unchecked<1>();
    auto time_residual_view = time_residual.mutable_unchecked<1>();

    // collect info to calculate timestamp mean per detector_id
    std::map<int, std::pair<float, int>> m_timestamp_avg;
    for(int i = 0; i < nrows; i++){
        m_timestamp_avg[ detector_id_view(i) ].first += timestamp_view(i);
        m_timestamp_avg[ detector_id_view(i) ].second += 1;
    }
    
    for(int i = 0; i < nrows; i++){
        radius_view(i) = std::sqrt(  x_view(i)*x_view(i) + y_view(i)*y_view(i) );
        momentum_proxy_view(i) = energy_view(i) / (radius_view(i) + epsilon);
        time_residual_view(i) = timestamp_view(i) - m_timestamp_avg[ detector_id_view(i) ].first /  m_timestamp_avg[ detector_id_view(i) ].second;
    }

    return {{"radius", radius}, {"momentum_proxy", momentum_proxy}, {"time_residual", time_residual}};

}

// Gotta add filter


// Python module definition
PYBIND11_MODULE(_processor, m) {
    m.doc() = "C++ extension for EventProcessor";
    m.def("compute_newvars", &compute_newvars, "A function compute new variables for the dataframe");
}
