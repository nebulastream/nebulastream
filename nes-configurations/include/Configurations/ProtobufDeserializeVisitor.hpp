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
#include <Configurations/OptionVisitor.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/util/json_util.h>

namespace NES::Configurations
{
class ProtobufDeserializeVisitor : public NES::Configurations::OptionVisitor
{
public:
    explicit ProtobufDeserializeVisitor(google::protobuf::Message* message)
    {
        const google::protobuf::Descriptor* descriptor = message->GetDescriptor();
        reflections.push(message->GetReflection());
        messages.push(message);
        descriptors.push(descriptor);
    }

    void push(BaseOption& o) override
    {
        const auto* descriptor = descriptors.top()->FindFieldByName(o.getName());
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
        underlying = reflections.top()->GetUInt64(*messages.top(), fieldDescriptor);
    }
    void visitUnsignedInteger(size_t& v) override { v = reflections.top()->GetUInt64(*messages.top(), fieldDescriptor); }
    void visitSignedInteger(ssize_t& v) override { v = reflections.top()->GetInt64(*messages.top(), fieldDescriptor); }
    void visitFloat(double& v) override { v = reflections.top()->GetDouble(*messages.top(), fieldDescriptor); }
    void visitBool(bool& v) override { v = reflections.top()->GetBool(*messages.top(), fieldDescriptor); }
    void visitString(std::string& s) override { s = reflections.top()->GetString(*messages.top(), fieldDescriptor); }

private:
    const google::protobuf::FieldDescriptor* fieldDescriptor{};
    std::stack<google::protobuf::Message*> messages;
    std::stack<const google::protobuf::Reflection*> reflections;
    std::stack<const google::protobuf::Descriptor*> descriptors;
};
}
