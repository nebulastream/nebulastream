/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
