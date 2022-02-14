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

#ifndef NES_INCLUDE_MONITORING_COLLECTORS_METRICCOLLECTOR_HPP_
#define NES_INCLUDE_MONITORING_COLLECTORS_METRICCOLLECTOR_HPP_

#include <Runtime/RuntimeForwardRefs.hpp>
#include <memory>

namespace NES {

class Metric;
using MetricPtr = std::shared_ptr<Metric>;

class MetricCollector {
  protected:
    //  -- Constructors --
    MetricCollector() = default;
    MetricCollector(const MetricCollector&) = default;
    MetricCollector(MetricCollector&&) = default;
    //  -- Assignment --
    MetricCollector& operator=(const MetricCollector&) = default;
    MetricCollector& operator=(MetricCollector&&) = default;

  public:
    //  -- Destructor --
    virtual ~MetricCollector() = default;

    /**
     * @brief Fill a buffer with a given metric.
     * @param tupleBuffer The tuple buffer
     * @return True if successful, else false
    */
    virtual bool fillBuffer(Runtime::TupleBuffer& tupleBuffer) = 0;

    /**
     * @brief Return the schema representing the metrics gathered by the collector.
     * @return The schema
    */
    virtual SchemaPtr getSchema() = 0;

    /**
     * @brief Fill a buffer with a given metric.
     * @param tupleBuffer The tuple buffer
     * @return True if successful, else false
     */
    virtual MetricPtr readMetric() = 0;
};

using MetricCollectorPtr = std::shared_ptr<MetricCollector>;

}// namespace NES

#endif//NES_INCLUDE_MONITORING_COLLECTORS_METRICCOLLECTOR_HPP_