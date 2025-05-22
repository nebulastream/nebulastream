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
#include <stack>
#include <string>
#include <string_view>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ISequenceOption.hpp>
#include <Configurations/ReadingVisitor.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>
#include <sys/types.h>
#include <ErrorHandling.hpp>

namespace NES::Configurations
{

/// Serializes all options of a configuration. The basic structure (message) is always given by
/// ProtobufMessageTypeBuilderOptionVisitor, the focus in this class is only on writing the config values into the message.
/// Uses protobuf reflection util to dynamically fill a protobuf message with the contents of a configuration option.
class ProtobufSerializeOptionVisitor : public NES::Configurations::ReadingVisitor
{
public:
    explicit ProtobufSerializeOptionVisitor(google::protobuf::Message* message)
    {
        const google::protobuf::Descriptor* descriptor = message->GetDescriptor();
        reflections.push(message->GetReflection());
        messages.push(message);
        descriptors.push(descriptor);
    }


    void push(const ISequenceOption& option) override
    {
        inSequence.push(true);
        fieldDescriptor = descriptors.top()->FindFieldByName(option.getName());
    }
    void pop(const ISequenceOption&) override
    {
        inSequence.pop();
    }
    void push(const BaseOption& option) override
    {
        /// The top level configuration name is meaning less. Thus we always use root
        auto optionName = option.getName();
        if (descriptors.size() == 1)
        {
            optionName = "root";
        }

        const auto* descriptor = descriptors.top()->FindFieldByName(optionName);
        if (descriptor == nullptr)
        {
            throw InvalidConfigParameter("Field not found: {}", optionName);
        }

        auto* message = reflections.top()->MutableMessage(messages.top(), descriptor);
        reflections.push(message->GetReflection());
        messages.push(message);
        descriptors.push(message->GetDescriptor());
        inSequence.push(false);
    }

    void pop(const BaseOption&) override
    {
        inSequence.pop();
        reflections.pop();
        messages.pop();
        descriptors.pop();
    }

protected:
    void visitLeaf(const BaseOption& option) override
    {
        if (!inSequence.top())
        {
            fieldDescriptor = descriptors.top()->FindFieldByName(option.getName());
        }
    }
    void visitEnum(std::string_view, const size_t& value) override { visitUnsignedInteger(value); }
    void visitUnsignedInteger(const size_t& value) override
    {
        if (inSequence.top())
        {
            reflections.top()->AddUInt64(messages.top(), fieldDescriptor, value);
        }
        else
        {
            reflections.top()->SetUInt64(messages.top(), fieldDescriptor, value);
        }
    }
    void visitSignedInteger(const ssize_t& value) override
    {
        if (inSequence.top())
        {
            reflections.top()->AddInt64(messages.top(), fieldDescriptor, value);
        }
        else
        {
            reflections.top()->SetInt64(messages.top(), fieldDescriptor, value);
        }
    }
    void visitDouble(const double& value) override
    {
        if (inSequence.top())
        {
            reflections.top()->AddDouble(messages.top(), fieldDescriptor, value);
        }
        else
        {
            reflections.top()->SetDouble(messages.top(), fieldDescriptor, value);
        }
    }
    void visitBool(const bool& value) override
    {
        if (inSequence.top())
        {
            reflections.top()->AddBool(messages.top(), fieldDescriptor, value);
        }
        else
        {
            reflections.top()->SetBool(messages.top(), fieldDescriptor, value);
        }
    }
    void visitString(const std::string& value) override
    {
        if (inSequence.top())
        {
            reflections.top()->AddString(messages.top(), fieldDescriptor, value);
        }
        else
        {
            reflections.top()->SetString(messages.top(), fieldDescriptor, value);
        }
    }

private:
    const google::protobuf::FieldDescriptor* fieldDescriptor{};
    std::stack<google::protobuf::Message*> messages;
    std::stack<const google::protobuf::Reflection*> reflections;
    std::stack<const google::protobuf::Descriptor*> descriptors;
    std::stack<bool> inSequence;
};
}
