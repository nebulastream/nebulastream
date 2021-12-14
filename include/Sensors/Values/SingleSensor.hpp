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

#ifndef NES_INCLUDE_SENSORS_VALUES_SINGLE_VALUE_HPP_
#define NES_INCLUDE_SENSORS_VALUES_SINGLE_VALUE_HPP_

#include <cstring>

namespace NES {
namespace Sensors {

struct __attribute__((packed)) SingleSensor {
    uint64_t id;
    uint64_t payload;
    uint64_t timestamp;
    double sensedValue;

    SingleSensor() {
        id = 0;
        payload = 0;
        timestamp = 0;
        sensedValue = -1;
    }

    SingleSensor(const SingleSensor& rhs) {
        id = rhs.id;
        payload = rhs.payload;
        timestamp = rhs.timestamp;
        sensedValue = rhs.sensedValue;
    }
};

}// namespace Sensors
}// namespace NES

#endif//NES_INCLUDE_SENSORS_VALUES_SINGLE_VALUE_HPP_
