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

#include <string>
#include <vector>
#include <DataTypes/LogicalType.hpp>
#include <DataTypes/Nullable.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using LogicalTypeRegistryReturnType = LogicalType;

struct LogicalTypeRegistryArguments
{
    Nullable nullable{Nullable::IS_NULLABLE};
    std::vector<LogicalType::Parameter> parameters;
};

class LogicalTypeRegistry
    : public BaseRegistry<LogicalTypeRegistry, std::string, LogicalTypeRegistryReturnType, LogicalTypeRegistryArguments>
{
};

}

#define INCLUDED_FROM_LOGICAL_TYPE_REGISTRY
#include <LogicalTypeGeneratedRegistrar.inc>
#undef INCLUDED_FROM_LOGICAL_TYPE_REGISTRY
