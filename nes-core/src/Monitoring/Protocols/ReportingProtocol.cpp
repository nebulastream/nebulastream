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

#include <Monitoring/Protocols/ReportingProtocol.hpp>

#include <Util/Logger.hpp>
namespace NES {

ReportingProtocol::ReportingProtocol(std::function<void(MetricGroup&)>&& reportingFunc) : reportingFunc(reportingFunc) {
    NES_DEBUG("ReportingProtocol: Init()");
}

bool ReportingProtocol::canReceive() const { return receiving; }

void ReportingProtocol::receive(MetricGroup& metricGroup) {
    if (receiving) {
        NES_DEBUG("ReportingProtocol: Receiving metrics");
        reportingFunc(metricGroup);
    } else {
        NES_ERROR("ReportingProtocol: Metrics received, but not ready for receiving");
    }
}

}// namespace NES