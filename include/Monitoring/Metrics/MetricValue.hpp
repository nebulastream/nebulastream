#ifndef NES_INCLUDE_MONITORING_METRICS_METRICVALUE_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICVALUE_HPP_

#include <memory>

namespace NES {

enum MetricValueReturnType {
    uInt64,
    int64,
    string
};

class MetricValue {
  public:
    virtual ~MetricValue() = default;
    virtual std::unique_ptr<MetricValue> copy() const = 0;
    virtual std::string getValue() const = 0;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_METRICVALUE_HPP_
