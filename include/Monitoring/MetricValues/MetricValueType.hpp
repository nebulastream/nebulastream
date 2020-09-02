#ifndef NES_INCLUDE_MONITORING_METRICVALUES_METRICVALUETYPE_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_METRICVALUETYPE_HPP_

namespace NES {

/**
 * @brief The metric value types of NES
 */
enum MetricValueType {
    CpuMetric,
    DiskMetric,
    MemoryMetric,
    NetworkMetric
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_METRICVALUETYPE_HPP_
