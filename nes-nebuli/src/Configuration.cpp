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

#include <Configuration.hpp>

#include <utility>
#include <ErrorHandling.hpp>
#include "Common/DataTypes/DataTypeFactory.hpp"

NES::Distributed::Config::Topology NES::Distributed::Config::Topology::from(CLI::QueryConfig config, ConnectionIdentifier singleNodeServer)
{
    return Topology{
        .nodes = {Node{
            .grpc = std::move(singleNodeServer),
            .connection = "",
            .sinks = std::move(config.sinks),
            .physical = std::move(config.physical),
            .capacity = std::numeric_limits<size_t>::max(),
            .links = Links{}}},
        .logical = std::move(config.logical)};
}

namespace NES::CLI
{
std::shared_ptr<DataType> stringToFieldType(const std::string& fieldNodeType)
{
    if (fieldNodeType == "VARSIZED")
    {
        return DataTypeFactory::createVariableSizedData();
    }

    if (fieldNodeType == "BOOLEAN")
    {
        return DataTypeFactory::createBoolean();
    }

    if (fieldNodeType == "INT8")
    {
        return DataTypeFactory::createInt8();
    }

    if (fieldNodeType == "UINT8")
    {
        return DataTypeFactory::createUInt8();
    }

    if (fieldNodeType == "INT16")
    {
        return DataTypeFactory::createInt16();
    }

    if (fieldNodeType == "UINT16")
    {
        return DataTypeFactory::createUInt16();
    }

    if (fieldNodeType == "INT32")
    {
        return DataTypeFactory::createInt32();
    }

    if (fieldNodeType == "UINT32")
    {
        return DataTypeFactory::createUInt32();
    }

    if (fieldNodeType == "INT64")
    {
        return DataTypeFactory::createInt64();
    }

    if (fieldNodeType == "UINT64")
    {
        return DataTypeFactory::createUInt64();
    }

    if (fieldNodeType == "FLOAT32")
    {
        return DataTypeFactory::createFloat();
    }

    if (fieldNodeType == "FLOAT64")
    {
        return DataTypeFactory::createDouble();
    }

    if (fieldNodeType == "CHAR")
    {
        return DataTypeFactory::createChar();
    }

    throw SLTWrongSchema("Found Invalid Logical Source Configuration. {} is not a proper Schema Field Type.", fieldNodeType);
}
}

NES::CLI::SchemaField::SchemaField(std::string name, std::string typeName) : SchemaField(std::move(name), CLI::stringToFieldType(typeName))
{
}

NES::CLI::SchemaField::SchemaField(std::string name, std::shared_ptr<NES::DataType> type) : name(std::move(name)), type(std::move(type))
{
}
