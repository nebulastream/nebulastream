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
#include <iostream>
#include <typeindex>
#include <unordered_map>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>

#include <Configurations/ProtobufMessageTypeBuilderOptionVisitor.hpp>

#include "ProtobufDeserializeVisitor.hpp"
#include "ProtobufSerializeOptionVisitor.hpp"

/// We do not want rebuild every protobuf message type whenever we want to serialize a configuration.
/// This class caches protobuf message types and provides a convenient way to create a new protobuf message based on a configuration class.
class ProtobufConfigCache
{
public:
    ProtobufConfigCache() { protoFile.set_name("configurations"); }

    template <typename ConfigurationClass>
    google::protobuf::Message* getEmptyMessage()
    {
        if (const auto iterator = typeIndexToConfigClassTypeName.find(std::type_index(typeid(ConfigurationClass)));
            iterator != typeIndexToConfigClassTypeName.end())
        {
            const auto* fileDesc = pool.BuildFile(protoFile);
            const google::protobuf::Descriptor* messageDesc = fileDesc->FindMessageTypeByName(iterator->second);
            return factory.GetPrototype(messageDesc)->New();
        }
        /// Inserts names and types of options into protoFile
        NES::Configurations::ProtobufMessageTypeBuilderOptionVisitor visitor(protoFile, typeid(ConfigurationClass).name());
        ConfigurationClass configuration("root", "");
        configuration.accept(visitor);
        typeIndexToConfigClassTypeName[std::type_index(typeid(ConfigurationClass))] = typeid(ConfigurationClass).name();
        return getEmptyMessage<ConfigurationClass>();
    }

    template <typename ConfigurationClass>
    ConfigurationClass deserialize(google::protobuf::Message* message)
    {
        ConfigurationClass configuration("root", "");
        NES::Configurations::ProtobufDeserializeVisitor serializer(message);
        configuration.accept(serializer);
        return configuration;
    }

    template <typename ConfigurationClass>
    google::protobuf::Message* serialize(const ConfigurationClass& config)
    {
        auto* message = getEmptyMessage<ConfigurationClass>();
        NES::Configurations::ProtobufSerializeOptionVisitor serializer(message);
        config.accept(serializer);
        return message;
    }

    void printFile()
    {
        std::string jsonFormat;
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.always_print_primitive_fields = true;
        auto status = google::protobuf::util::MessageToJsonString(protoFile, &jsonFormat, options);
        std::cout << "=== Schema in JSON Format ===\n" << jsonFormat << "\n\n";
    }

private:
    google::protobuf::DescriptorPool pool;
    google::protobuf::DynamicMessageFactory factory;
    google::protobuf::FileDescriptorProto protoFile;
    std::unordered_map<std::type_index, std::string> typeIndexToConfigClassTypeName;
};
