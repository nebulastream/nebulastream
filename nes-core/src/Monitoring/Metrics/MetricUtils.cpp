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

#include "Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp"
#include "Monitoring/ResourcesReader/LinuxSystemResourcesReader.hpp"
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/CpuValues.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/MetricValues/RuntimeNesMetrics.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

AbstractSystemResourcesReaderPtr MetricUtils::getSystemResourcesReader() {
#ifdef __linux__
    auto abstractReader = std::make_shared<LinuxSystemResourcesReader>();
    NES_INFO("MetricUtils: Linux detected, return LinuxSystemResourcesReader");
#else
    auto abstractReader = std::make_shared<AbstractSystemResourcesReader>();
    NES_INFO("MetricUtils: OS not supported, return DefaultReader");
#endif

    return abstractReader;
};

Gauge<RuntimeNesMetrics> MetricUtils::runtimeNesStats() {
    return Gauge<RuntimeNesMetrics>([]() {
        return getSystemResourcesReader()->readRuntimeNesMetrics();
    });
}

Gauge<StaticNesMetrics> MetricUtils::staticNesStats() {
    return Gauge<StaticNesMetrics>([]() {
        return getSystemResourcesReader()->readStaticNesMetrics();
    });
}

Gauge<CpuMetrics> MetricUtils::cpuStats() {
    return Gauge<CpuMetrics>([]() {
        return getSystemResourcesReader()->readCpuStats();
    });
}

Gauge<MemoryMetrics> MetricUtils::memoryStats() {
    return Gauge<MemoryMetrics>([]() {
        return getSystemResourcesReader()->readMemoryStats();
    });
}

Gauge<DiskMetrics> MetricUtils::diskStats() {
    return Gauge<DiskMetrics>([]() {
        return getSystemResourcesReader()->readDiskStats();
    });
}

Gauge<NetworkMetrics> MetricUtils::networkStats() {
    return Gauge<NetworkMetrics>([]() {
        return getSystemResourcesReader()->readNetworkStats();
    });
}

Gauge<uint64_t> MetricUtils::cpuIdle(unsigned int cpuNo) {
    auto gaugeIdle = Gauge<uint64_t>([cpuNo]() {
        NES_DEBUG("MetricUtils: Reading CPU Idle for cpu " << cpuNo);
        return getSystemResourcesReader()->readCpuStats().getValues(cpuNo).idle;
    });
    return gaugeIdle;
}

bool MetricUtils::validateFieldsInSchema(SchemaPtr metricSchema, SchemaPtr bufferSchema, uint64_t i) {
    if (i >= bufferSchema->getSize()) {
        return false;
    }

    auto hasName = Util::endsWith(bufferSchema->fields[i]->getName(), metricSchema->get(0)->getName());
    auto hasLastField = Util::endsWith(bufferSchema->fields[i + metricSchema->getSize() - 1]->getName(),
                                       metricSchema->get(metricSchema->getSize() - 1)->getName());

    if (!hasName || !hasLastField) {
        return false;
    }
    return true;
}

}// namespace NES