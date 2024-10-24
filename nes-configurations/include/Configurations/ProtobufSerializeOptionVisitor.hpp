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
#include <Configurations/BaseOption.hpp>
#include <Configurations/OptionVisitor.hpp>
#include <Configurations/Validation/BooleanValidation.hpp>
#include <Configurations/Validation/FloatValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/json_util.h>

namespace NES::Configurations
{

class ProtobufSerializeOptionVisitor : public NES::Configurations::OptionVisitor
{
public:
    explicit ProtobufSerializeOptionVisitor(google::protobuf::Message* message)
    {
        const google::protobuf::Descriptor* descriptor = message->GetDescriptor();
        reflections.push(message->GetReflection());
        messages.push(message);
        descriptors.push(descriptor);
    }

    void push(BaseOption& o) override
    {
        const auto* descriptor = descriptors.top()->FindFieldByName(o.getName());
        if (!descriptor)
        {
            std::cerr << "Field not found: " << o.getName() << std::endl;
            assert(descriptor && "Field not found");
        }
        auto* message = reflections.top()->MutableMessage(messages.top(), descriptor);
        const auto* reflection = message->GetReflection();

        reflections.push(reflection);
        messages.push(message);
        descriptors.push(message->GetDescriptor());
    }
    void pop(BaseOption&) override
    {
        reflections.pop();
        messages.pop();
        descriptors.pop();
    }

protected:
    void visitLeaf(BaseOption& o) override { fieldDescriptor = descriptors.top()->FindFieldByName(o.getName()); }
    void visitEnum(std::string_view, size_t& underlying) override
    {
        reflections.top()->SetUInt64(messages.top(), fieldDescriptor, underlying);
    }
    void visitUnsignedInteger(size_t& v) override { reflections.top()->SetUInt64(messages.top(), fieldDescriptor, v); }
    void visitSignedInteger(ssize_t& v) override { reflections.top()->SetInt64(messages.top(), fieldDescriptor, v); }
    void visitFloat(double& v) override { reflections.top()->SetDouble(messages.top(), fieldDescriptor, v); }
    void visitBool(bool& v) override { reflections.top()->SetBool(messages.top(), fieldDescriptor, v); }
    void visitString(std::string& s) override { reflections.top()->SetString(messages.top(), fieldDescriptor, s); }

private:
    const google::protobuf::FieldDescriptor* fieldDescriptor{};
    std::stack<google::protobuf::Message*> messages;
    std::stack<const google::protobuf::Reflection*> reflections;
    std::stack<const google::protobuf::Descriptor*> descriptors;
};
}
