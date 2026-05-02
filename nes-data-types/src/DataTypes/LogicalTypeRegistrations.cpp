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

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterBOOLEANLogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("BOOLEAN", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterCHARLogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("CHAR", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterFLOAT32LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("FLOAT32", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterFLOAT64LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("FLOAT64", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterINT8LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("INT8", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterINT16LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("INT16", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterINT32LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("INT32", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterINT64LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("INT64", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterUINT8LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("UINT8", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterUINT16LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("UINT16", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterUINT32LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("UINT32", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterUINT64LogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("UINT64", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterUNDEFINEDLogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("UNDEFINED", std::move(args));
}

LogicalTypeRegistryReturnType LogicalTypeGeneratedRegistrar::RegisterVARSIZEDLogicalType(LogicalTypeRegistryArguments args)
{
    return makeBuiltin("VARSIZED", std::move(args));
}

}
