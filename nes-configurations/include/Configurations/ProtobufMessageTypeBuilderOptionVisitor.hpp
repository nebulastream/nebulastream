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
#include <Configurations/OptionVisitor.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>


namespace NES::Configurations
{
class ProtobufMessageTypeBuilderOptionVisitor : public OptionVisitor
{
public:
    ProtobufMessageTypeBuilderOptionVisitor(google::protobuf::FileDescriptorProto& proto, std::string configName) : protoFile(proto)
    {
        current.push(protoFile.add_message_type());
        current.top()->set_name(configName);
        fieldNumber.push(1);
    }

    void push(BaseOption& o) override
    {
        auto* field = current.top()->add_field();
        auto* type = current.top()->add_nested_type();

        type->set_name(o.getName() + "_type_name");

        field->set_name(o.getName());
        field->set_type(google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
        field->set_type_name(o.getName() + "_type_name");
        field->set_number(fieldNumber.top()++);

        currentField.push(field);
        current.push(type);
        fieldNumber.push(1);
    }

    void pop(BaseOption&) override
    {
        currentField.pop();
        current.pop();
        fieldNumber.pop();
    }

protected:
    void visitLeaf(BaseOption& o) override
    {
        auto* child = current.top();
        currentField.push(child->add_field());
        currentField.top()->set_name(o.getName());
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
};

}
