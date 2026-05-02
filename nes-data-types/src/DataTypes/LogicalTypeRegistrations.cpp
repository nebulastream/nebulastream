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

#include <utility>
#include <DataTypes/LogicalType.hpp>
#include <LogicalTypeRegistry.hpp>

namespace NES
{

namespace
{
LogicalType makeBuiltin(const std::string& name, LogicalTypeRegistryArguments arguments)
{
    return LogicalType{name, std::move(arguments.parameters), arguments.nullable};
}
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterINTEGERLogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("INTEGER", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterFLOATLogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("FLOAT", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterTEXTLogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("TEXT", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterBOOLLogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("BOOL", std::move(args));
}

}
