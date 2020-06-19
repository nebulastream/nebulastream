#include <Monitoring/Metrics/Gauge.hpp>
#include <Util/Logger.hpp>

namespace NES {

template<typename T>
Gauge<T>::Gauge(T value): value(value) {
    NES_DEBUG("Gauge: Initializing with value " << value);
}

template<typename T>
T Gauge<T>::getValue() {
    return value;
}

}