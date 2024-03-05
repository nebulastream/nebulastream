/*
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

#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddNodeEvent.hpp>

namespace NES::RequestProcessor {

ISQPAddNodeEvent::ISQPAddNodeEvent(NES::WorkerId workerId,
                                   const std::string& ipAddress,
                                   uint32_t grpcPort,
                                   uint32_t dataPort,
                                   uint16_t resources,
                                   const std::map<std::string, std::any>& properties)
    : ISQPEvent(ISQP_ADD_NODE_EVENT_PRIORITY), workerId(workerId), ipAddress(ipAddress), grpcPort(grpcPort), dataPort(dataPort),
      resources(resources), properties(properties) {}

ISQPEventPtr ISQPAddNodeEvent::create(NES::WorkerId workerId,
                                      const std::string& ipAddress,
                                      uint32_t grpcPort,
                                      uint32_t dataPort,
                                      uint16_t resources,
                                      const std::map<std::string, std::any>& properties) {
    return std::make_shared<ISQPAddNodeEvent>(workerId, ipAddress, grpcPort, dataPort, resources, properties);
}

WorkerId ISQPAddNodeEvent::getWorkerId() const { return workerId; }

const std::string& ISQPAddNodeEvent::getIpAddress() const { return ipAddress; }

uint32_t ISQPAddNodeEvent::getGrpcPort() const { return grpcPort; }

uint32_t ISQPAddNodeEvent::getDataPort() const { return dataPort; }

uint16_t ISQPAddNodeEvent::getResources() const { return resources; }

const std::map<std::string, std::any>& ISQPAddNodeEvent::getProperties() const { return properties; }

}// namespace NES::RequestProcessor
