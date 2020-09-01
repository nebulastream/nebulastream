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
    NetworkMetric,

    //not supported so far
    //UINT64Metric,
    //INT64Metric,
    //StringMetric
};

}


#endif//NES_INCLUDE_MONITORING_METRICVALUES_METRICVALUETYPE_HPP_
