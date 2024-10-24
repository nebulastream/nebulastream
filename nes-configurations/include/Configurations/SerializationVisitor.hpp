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
#include <memory>
#include <stack>
#include <Configurations/OptionVisitor.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>

class SerializationVisitor : public NES::Configurations::OptionVisitor
{
public:
    void push() override
    {
        current.push(protoFile.add_message_type());
        current.top()->set_name("A");
    }
    void pop() override
    {
        auto* child = current.top();
        current.pop();

        auto* newField = current.top()->add_field();
        newField->set_name("B");
        newField->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
        newField->set_type_name(child->name());
        newField->set_number(1);
    }

    void visitConcrete(std::string name, std::string, std::string_view) override
    {
        auto* new_field = current.top()->add_field();
        new_field->set_name(name);
        new_field->set_type(google::protobuf::FieldDescriptorProto::TYPE_INT32);
        new_field->set_number(1);
    }

    SerializationVisitor()
    {
        protoFile.set_name("config.proto");
        protoFile.add_message_type()->set_name("config");
        current.push(protoFile.add_message_type());
    };

    void print()
    {
        std::string json_format;
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.always_print_primitive_fields = true;
        auto _ = google::protobuf::util::MessageToJsonString(protoFile, &json_format, options);
        std::cout << "=== Schema in JSON Format ===\n" << json_format << "\n\n";
    }

private:
    std::stack<google::protobuf::DescriptorProto*> current;
    std::unique_ptr<google::protobuf::DescriptorPool> pool_ = std::make_unique<google::protobuf::DescriptorPool>();
    std::unique_ptr<google::protobuf::DynamicMessageFactory> factory_
        = std::make_unique<google::protobuf::DynamicMessageFactory>(pool_.get());
    google::protobuf::FileDescriptorProto protoFile;
};
