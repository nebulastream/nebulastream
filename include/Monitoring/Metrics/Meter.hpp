#ifndef NES_INCLUDE_MONITORING_METRICS_METER_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METER_HPP_

#include <Monitoring/Metrics/Metric.hpp>
#include <Util/Logger.hpp>

namespace NES {

/**
 * @brief TODO: WIP
 */
class Meter : public Metric {
    Meter(): Metric(MetricType::MeterType) {
        NES_NOT_IMPLEMENTED();
    }
};

}

#endif //NES_INCLUDE_MONITORING_METRICS_METER_HPP_
