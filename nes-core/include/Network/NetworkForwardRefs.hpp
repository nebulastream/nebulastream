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

#ifndef NES_INCLUDE_NETWORK_NETWORKFORWARDREFS_HPP_
#define NES_INCLUDE_NETWORK_NETWORKFORWARDREFS_HPP_

#include <Network/NesPartition.hpp>
#include <Network/NodeLocation.hpp>
#include <memory>

#define FORWARD_DECLARE_CLASS_UP(T)                                                                                              \
    class T;                                                                                                                     \
    using T##Ptr = std::unique_ptr<T>

#define FORWARD_DECLARE_CLASS_SP(T)                                                                                              \
    class T;                                                                                                                     \
    using T##Ptr = std::shared_ptr<T>

namespace NES::Network {
class ExchangeProtocol;
FORWARD_DECLARE_CLASS_UP(ZmqServer);
FORWARD_DECLARE_CLASS_UP(NetworkChannel);
FORWARD_DECLARE_CLASS_UP(EventOnlyNetworkChannel);
FORWARD_DECLARE_CLASS_SP(NetworkSource);
FORWARD_DECLARE_CLASS_SP(NetworkSink);
FORWARD_DECLARE_CLASS_SP(NetworkManager);
FORWARD_DECLARE_CLASS_SP(PartitionManager);
}// namespace NES::Network
#undef FORWARD_DECLARE_CLASS_UP
#undef FORWARD_DECLARE_CLASS_SP
#endif//NES_INCLUDE_NETWORK_NETWORKFORWARDREFS_HPP_
