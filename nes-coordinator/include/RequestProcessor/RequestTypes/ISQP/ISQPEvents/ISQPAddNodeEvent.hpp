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

#ifndef NES_ISQPADDTOPOLOGYNODEEVENT_HPP
#define NES_ISQPADDTOPOLOGYNODEEVENT_HPP

#include <Identifiers.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEvent.hpp>
#include <any>
#include <map>

namespace NES::RequestProcessor {

/**
 * @brief ISQP add node response indicating if the event was applied successfully or not
 */
struct ISQPAddNodeResponse : public ISQPResponse {
    explicit ISQPAddNodeResponse(bool success) : success(success){};
    bool success;
};
using ISQPAddNodeResponsePtr = std::shared_ptr<ISQPAddNodeResponse>;

class ISQPAddNodeEvent;
using ISQPAddNodeEventPtr = std::shared_ptr<ISQPAddNodeEvent>;

/**
 * @brief ISQP add node represent the event for registering a worker node
 */
class ISQPAddNodeEvent : public ISQPEvent {

  public:
    static ISQPEventPtr create(WorkerId workerId,
                               const std::string& ipAddress,
                               uint32_t grpcPort,
                               uint32_t dataPort,
                               uint16_t resources,
                               const std::map<std::string, std::any>& properties);

    ISQPAddNodeEvent(WorkerId workerId,
                     const std::string& ipAddress,
                     uint32_t grpcPort,
                     uint32_t dataPort,
                     uint16_t resources,
                     const std::map<std::string, std::any>& properties);

    WorkerId getWorkerId() const;

    const std::string& getIpAddress() const;

    uint32_t getGrpcPort() const;

    uint32_t getDataPort() const;

    uint16_t getResources() const;

    const std::map<std::string, std::any>& getProperties() const;

  private:
    WorkerId workerId;
    std::string ipAddress;
    uint32_t grpcPort;
    uint32_t dataPort;
    uint16_t resources;
    std::map<std::string, std::any> properties;
};
}// namespace NES::RequestProcessor

#endif//NES_ISQPADDTOPOLOGYNODEEVENT_HPP
