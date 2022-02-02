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

#ifndef NES_TESTS_UTIL_METRICVALIDATOR_HPP_
#define NES_TESTS_UTIL_METRICVALIDATOR_HPP_

#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/MetricValues/RuntimeNesMetrics.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/LinuxSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderType.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/Logger.hpp>

namespace NES {

class MetricValidator {
  public:
    static bool isValid(AbstractSystemResourcesReaderPtr reader, RuntimeNesMetrics metrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for RuntimeMetrics. Returning true");
            return true;
        }

        if (metrics.wallTimeNs <= 0) {
            NES_ERROR("MetricValidator: Wrong wallTimeNs.");
            check = false;
        }
        if (!(metrics.blkioBytesWritten >= 0)) {
            NES_ERROR("MetricValidator: Wrong blkioBytesWritten.");
            check = false;
        }
        if (!(metrics.blkioBytesRead >= 0)) {
            NES_ERROR("MetricValidator: Wrong blkioBytesRead.");
            check = false;
        }
        if (!(metrics.memoryUsageInBytes >= 0)) {
            NES_ERROR("MetricValidator: Wrong memoryUsageInBytes.");
            check = false;
        }
        if (!(metrics.cpuLoadInJiffies >= 0)) {
            NES_ERROR("MetricValidator: Wrong cpuLoadInJiffies.");
            check = false;
        }
        if (!(metrics.longCoord >= 0)) {
            NES_ERROR("MetricValidator: Wrong longCoord.");
            check = false;
        }
        if (!(metrics.latCoord >= 0)) {
            NES_ERROR("MetricValidator: Wrong latCoord.");
            check = false;
        }
        if (!(metrics.batteryStatusInPercent >= 0)) {
            NES_ERROR("MetricValidator: Wrong batteryStatusInPercent.");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, StaticNesMetrics metrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for StaticNesMetrics. Returning true");
            return true;
        }

        if (!(metrics.cpuQuotaUS >= -1)) {
            NES_ERROR("MetricValidator: Wrong cpuQuotaUS.");
            check = false;
        }
        if (!(metrics.cpuPeriodUS >= -1)) {
            NES_ERROR("MetricValidator: Wrong cpuPeriodUS.");
            check = false;
        }
        if (!(metrics.totalCPUJiffies > 0)) {
            NES_ERROR("MetricValidator: Wrong totalCPUJiffies.");
            check = false;
        }
        if (!(metrics.cpuCoreNum > 0)) {
            NES_ERROR("MetricValidator: Wrong cpuCoreNum.");
            check = false;
        }
        if (!(metrics.totalMemoryBytes >= 0)) {
            NES_ERROR("MetricValidator: Wrong totalMemoryBytes.");
            check = false;
        }
        if (metrics.hasBattery) {
            NES_ERROR("MetricValidator: Wrong hasBattery.");
            check = false;
        }
        if (metrics.isMoving) {
            NES_ERROR("MetricValidator: Wrong isMoving.");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, CpuMetrics cpuMetrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for CpuMetrics. Returning true");
            return true;
        }

        if (!(cpuMetrics.getNumCores() > 0)) {
            NES_ERROR("MetricValidator: Wrong getNumCores.");
            check = false;
        }
        for (int i = 0; i < cpuMetrics.getNumCores(); i++) {
            if (!(cpuMetrics.getValues(i).user > 0)) {
                NES_ERROR("MetricValidator: Wrong cpuMetrics.getValues(i).user.");
                check = false;
            }
        }
        if (!(cpuMetrics.getTotal().user > 0)) {
            NES_ERROR("MetricValidator: Wrong cpuMetrics.getTotal().user.");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, NetworkMetrics networkMetrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for NetworkMetrics. Returning true");
            return true;
        }

        if (networkMetrics.getInterfaceNames().empty()) {
            NES_ERROR("MetricValidator: Wrong getInterfaceNames().");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, MemoryMetrics memoryMetrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for MemoryMetrics. Returning true");
            return true;
        }

        if (!(memoryMetrics.FREE_RAM > 0)) {
            NES_ERROR("MetricValidator: Wrong FREE_RAM.");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, DiskMetrics diskMetrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for DiskMetrics. Returning true");
            return true;
        }

        if (!(diskMetrics.fBavail >= 0)) {
            NES_ERROR("MetricValidator: Wrong fBavail.");
            check = false;
        }
        return check;
    }
};

}// namespace NES

#endif//NES_TESTS_UTIL_METRICVALIDATOR_HPP_