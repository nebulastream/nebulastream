#ifndef NES_INCLUDE_MONITORING_METRICS_METRICTYPE_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICTYPE_HPP_

namespace NES {

/**
 * @brief The metric types of NES
 * Counter for incrementing and decrementing values
 * Gauge for reading and returning a specific value
 * Histogram that creates a histogram over time
 * Meter that measures an interval between two points in time
 * Unknown is used for all kind of metrics that are not defined during creation, e.g. in case basic types are used
 */
enum MetricType {
    CounterType,
    GaugeType,
    HistogramType,
    MeterType,
    UnknownType
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_METRICTYPE_HPP_
