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
#ifndef NES_INCLUDE_NETWORK_PARTITIONREGISTRATIONSTATUS_HPP_
#define NES_INCLUDE_NETWORK_PARTITIONREGISTRATIONSTATUS_HPP_
#include <cstdint>
namespace NES::Network {
/**
 * @brief This enum represent the life-cycle of a nes partition in the partition manager:
 * NotFound: partition was never registered in the current partition manager.
 * Deleted: partition was unregistered at some point in time (we keep it for sequenceNumberTracker).
 * Registered: partition was registered and alive (meaning its ref cnt is greater than 0).
 */
enum class PartitionRegistrationStatus : uint8_t {
    /// a partition is registered, i.e., its counter is >= 0
    Registered,
    /// a partition was registered but got deleted, i.e., its counter is == 0
    Deleted,
    /// a partition was never registered
    NotFound,
};
}// namespace NES::Network
#endif//NES_INCLUDE_NETWORK_PARTITIONREGISTRATIONSTATUS_HPP_
