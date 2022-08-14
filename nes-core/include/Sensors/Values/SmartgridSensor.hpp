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

#ifndef NES_INCLUDE_SENSORS_VALUES_SINGLESENSOR_HPP_
#define NES_INCLUDE_SENSORS_VALUES_SINGLESENSOR_HPP_

#include <cstring>

namespace NES {
namespace Sensors {

/**
 * @brief: the purpose of this struct is to encapsulate the
 * simplest schema coming out of a sensor, for testing and
 * benchmarking setups. Usually we need to read the
 * values and feed them to a strategy, e.g.: a Kalman filter.
 * Basically this is a small-time substitute instead
 * of passing around a schema that is not defined
 * across the codebase.
 */
struct __attribute__((packed)) SmartgridSensor {
    uint64_t timestamp;
    uint64_t value;
    uint64_t property;
    uint64_t id;
    uint64_t household;
    uint64_t house;

    // default c-tor
    SmartgridSensor() {
        timestamp = 0;
        value = 0;
        property = 0;
        id = 0;
        household = 0;
        house = 0;
    }

    SmartgridSensor(const SmartgridSensor& rhs) {
        timestamp = rhs.timestamp;
        value = rhs.value;
        property = rhs.property;
        id = rhs.id;
        household = rhs.household;
        house = rhs.house;
    }
};

}// namespace Sensors
}// namespace NES

#endif// NES_INCLUDE_SENSORS_VALUES_SINGLESENSOR_HPP_
