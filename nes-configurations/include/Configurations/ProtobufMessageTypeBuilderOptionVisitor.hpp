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
#include <cstdint>
#include <iostream>
#include <memory>
#include <stack>
#include <string>
#include <string_view>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ISequenceOption.hpp>
#include <Configurations/ReadingVisitor.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>
#include <sys/types.h>

namespace NES::Configurations
{

/// Inserts a new Protobuf Message type into the FileDescriptorProto. The Message type can be loaded and instantiated into
/// a new emtpy protobuf message using a DynamicMessageFactory
class ProtobufMessageTypeBuilderOptionVisitor : public ReadingVisitor
{
public:
    ProtobufMessageTypeBuilderOptionVisitor(google::protobuf::FileDescriptorProto& proto, std::string configName) : protoFile(&proto)
    {
        current.push(protoFile->add_message_type());
        current.top()->set_name(configName);
        fieldNumber.push(INITIAL_FIELD_NUMBER);
    }

    void printFile()
    {
        std::string jsonFormat;
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = false;
        options.always_print_primitive_fields = false;
        auto status = google::protobuf::util::MessageToJsonString(*protoFile, &jsonFormat, options);
        std::cout << "=== Schema in JSON Format ===\n" << jsonFormat << "\n\n";
    }

    std::stack<bool> inSequence;
    void push(const ISequenceOption& sequenceOption) override
    {
        auto defaultValue = sequenceOption.defaultValue();
        defaultValue->accept(*this);
        currentField.top()->set_label(google::protobuf::FieldDescriptorProto_Label_LABEL_REPEATED);
        inSequence.push(true);
    }
    void pop(const ISequenceOption&) override { inSequence.pop(); }

    void push(const BaseOption& option) override
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
        fieldNumber.push(INITIAL_FIELD_NUMBER);

        inSequence.push(false);
    }

    void pop(const BaseOption&) override
    {
        inSequence.pop();
        currentField.pop();
        current.pop();
        fieldNumber.pop();
    }

protected:
    void visitLeaf(const BaseOption& option) override
    {
        if (inSequence.top())
        {
            /// We don't actually care about concrete values of the sequence.
            /// push(Sequence) will create a single default value which is used to determine the schema.
            return;
        }

        auto* child = current.top();
        currentField.push(child->add_field());
        currentField.top()->set_name(option.getName());
        currentField.top()->set_number(fieldNumber.top()++);
    }

    void visitEnum(std::string_view, const size_t&) override
    {
        currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_UINT64);
    }
    void visitUnsignedInteger(const size_t&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_UINT64); }
    void visitSignedInteger(const ssize_t&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_INT64); }
    void visitDouble(const double&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_DOUBLE); }
    void visitBool(const bool&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_BOOL); }
    void visitString(const std::string&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_STRING); }


private:
    std::stack<google::protobuf::DescriptorProto*> current;
    std::stack<google::protobuf::FieldDescriptorProto*> currentField;
    std::stack<int32_t> fieldNumber;
    std::unique_ptr<google::protobuf::DescriptorPool> pool = std::make_unique<google::protobuf::DescriptorPool>();
    std::unique_ptr<google::protobuf::DynamicMessageFactory> factory
        = std::make_unique<google::protobuf::DynamicMessageFactory>(pool.get());
    google::protobuf::FileDescriptorProto* protoFile;
    static constexpr int32_t INITIAL_FIELD_NUMBER = 1;
};

}
