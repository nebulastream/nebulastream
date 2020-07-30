#ifndef NES_INCLUDE_MONITORING_METRICS_METRICVALUE_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICVALUE_HPP_

namespace NES {

class MetricValue {
  public:
    explicit MetricValue(void* value);
    ~MetricValue() = default;

    template<typename T>
    T get();

  private:
    void* value;

};

}

#endif //NES_INCLUDE_MONITORING_METRICS_METRICVALUE_HPP_
