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

#pragma once

#include <cstddef>
#include <vector>
#include <Identifiers/NESStrongType.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{

using HostAddr = NESStrongStringType<struct HostAddr_, "INVALID">;
using GrpcAddr = NESStrongStringType<struct GrpcAddr_, "INVALID">;

struct WorkerConfig
{
    HostAddr host;
    GrpcAddr grpc;
    size_t capacity;
    std::vector<HostAddr> downstream;
    SingleNodeWorkerConfiguration config;
};

}
