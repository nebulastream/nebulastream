#include <Monitoring/Metrics/MetricValue.hpp>

namespace NES {

MetricValue::MetricValue(void* value) : value(value) {
}

template<typename T>
T MetricValue::get() {
    return *static_cast<T*>(value);
}

}// namespace NES