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
#include <stack>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ReadingVisitor.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <gtest/internal/gtest-internal.h>

#include "ScalarOption.hpp"
#include "SequenceOption.hpp"


namespace NES::Configurations
{

/// Inserts a new Protobuf Message type into the FileDescriptorProto. The Message type can be loaded and instantiated into
/// a new emtpy protobuf message using a DynamicMessageFactory
class ProtobufMessageTypeBuilderOptionVisitor : public ReadingVisitor
{
public:
    ProtobufMessageTypeBuilderOptionVisitor(google::protobuf::FileDescriptorProto& proto, std::string configName) : protoFile(proto)
    {
        current.push(protoFile.add_message_type());
        current.top()->set_name(configName);
        fieldNumber.push(1);
        nameCounter = 1;
    }

    void printFile()
    {
        std::string json_format;
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = false;
        options.always_print_primitive_fields = false;
        auto _ = google::protobuf::util::MessageToJsonString(protoFile, &json_format, options);
        std::cout << "=== Schema in JSON Format ===\n" << json_format << "\n\n";
    }

    void push(BaseOption& option) override
    {
        std::string fieldName = option.getName();
        std::string typeName = option.getName() + "_type_name";

        auto* field = current.top()->add_field();
        field->set_name(fieldName);
        field->set_type(google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
        field->set_type_name(typeName);
        field->set_number(fieldNumber.top()++);

        auto* type = current.top()->add_nested_type();
        type->set_name(typeName);

        currentField.push(field);
        current.push(type);
        fieldNumber.push(1);

        if (auto* sequenceOption = dynamic_cast<SequenceOption<StringOption>*>(&option))
        {
            for (size_t i = 0; i < sequenceOption->size(); ++i)
            {
                push(sequenceOption->operator[](i));
                sequenceOption->operator[](i).accept(*this);
                pop(sequenceOption->operator[](i));
            }
        }
        else if (auto* sequenceOption = dynamic_cast<SequenceOption<FloatOption>*>(&option))
        {
            for (size_t i = 0; i < sequenceOption->size(); ++i)
            {
                push(sequenceOption->operator[](i));
                sequenceOption->operator[](i).accept(*this);
                pop(sequenceOption->operator[](i));
            }
        }
        else if (auto* sequenceOption = dynamic_cast<SequenceOption<BoolOption>*>(&option))
        {
            for (size_t i = 0; i < sequenceOption->size(); ++i)
            {
                push(sequenceOption->operator[](i));
                sequenceOption->operator[](i).accept(*this);
                pop(sequenceOption->operator[](i));
            }
        }
        else if (auto* sequenceOption = dynamic_cast<SequenceOption<UIntOption>*>(&option))
        {
            for (size_t i = 0; i < sequenceOption->size(); ++i)
            {
                push(sequenceOption->operator[](i));
                sequenceOption->operator[](i).accept(*this);
                pop(sequenceOption->operator[](i));
            }
        }
    }

    void pop(BaseOption&) override
    {
        currentField.pop();
        current.pop();
        fieldNumber.pop();
    }

protected:
    void visitLeaf(BaseOption& option) override
    {
        auto* child = current.top();
        currentField.push(child->add_field());
        currentField.top()->set_name(option.getName());
        currentField.top()->set_number(fieldNumber.top()++);
    }

    void visitEnum(std::string_view, size_t&) override
    {
        currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_UINT64);
    }
    void visitUnsignedInteger(size_t&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_UINT64); }
    void visitSignedInteger(ssize_t&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_INT64); }
    void visitFloat(double&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_DOUBLE); }
    void visitBool(bool&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_BOOL); }
    void visitString(std::string&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_STRING); }


private:
    std::stack<google::protobuf::DescriptorProto*> current;
    std::stack<google::protobuf::FieldDescriptorProto*> currentField;
    std::stack<size_t> fieldNumber;
    std::unique_ptr<google::protobuf::DescriptorPool> pool = std::make_unique<google::protobuf::DescriptorPool>();
    std::unique_ptr<google::protobuf::DynamicMessageFactory> factory
        = std::make_unique<google::protobuf::DynamicMessageFactory>(pool.get());
    google::protobuf::FileDescriptorProto& protoFile;
    size_t nameCounter;
};

}
