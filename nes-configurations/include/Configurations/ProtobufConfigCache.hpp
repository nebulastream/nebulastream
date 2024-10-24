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
#include <typeindex>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include "ProtobufMessageTypeBuilderOptionVisitor.hpp"

class ProtobufConfigCache
{
public:
    ProtobufConfigCache() { protoFile.set_name("configurations"); }

    template <typename ConfigurationClass>
    google::protobuf::Message* getEmptyMessage()
    {
        if (auto it = configNames.find(std::type_index(typeid(ConfigurationClass))); it != configNames.end())
        {
            auto fileDesc = pool.BuildFile(protoFile);
            const google::protobuf::Descriptor* messageDesc = fileDesc->FindMessageTypeByName(it->second);
            return factory.GetPrototype(messageDesc)->New();
        }

        NES::Configurations::ProtobufMessageTypeBuilderOptionVisitor option(protoFile, typeid(ConfigurationClass).name());
        ConfigurationClass configuration("root", "");
        configuration.accept(option);
        configNames[std::type_index(typeid(ConfigurationClass))] = typeid(ConfigurationClass).name();
        return getEmptyMessage<ConfigurationClass>();
    }

private:
    google::protobuf::DescriptorPool pool;
    google::protobuf::DynamicMessageFactory factory;
    google::protobuf::FileDescriptorProto protoFile;
    std::unordered_map<std::type_index, std::string> configNames;
};
