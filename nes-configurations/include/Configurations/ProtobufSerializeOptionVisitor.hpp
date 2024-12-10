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
#include "ScalarOption.hpp"


#include <stack>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ReadingVisitor.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/text_format.h>
#include <ErrorHandling.hpp>

namespace NES::Configurations
{

/// Serializes all options of a configuration. The basic structure is always given by
/// ProtobufMessageTypeBuilderOptionVisitor, the focus in this class is only on the config values.
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

    void push(BaseOption& option) override
    {
        const auto* descriptor = descriptors.top()->FindFieldByName(option.getName());
        if (!descriptor)
        {
            throw InvalidConfigParameter("Field not found: {}", option.getName());
        }

        auto* message = reflections.top()->MutableMessage(messages.top(), descriptor);
        reflections.push(message->GetReflection());
        messages.push(message);
        descriptors.push(message->GetDescriptor());

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
        reflections.pop();
        messages.pop();
        descriptors.pop();
    }

protected:
    void visitLeaf(BaseOption& option) override { fieldDescriptor = descriptors.top()->FindFieldByName(option.getName()); }
    void visitEnum(std::string_view, size_t& underlying) override
    {
        reflections.top()->SetUInt64(messages.top(), fieldDescriptor, underlying);
    }
    void visitUnsignedInteger(size_t& value) override { reflections.top()->SetUInt64(messages.top(), fieldDescriptor, value); }
    void visitSignedInteger(ssize_t& value) override { reflections.top()->SetInt64(messages.top(), fieldDescriptor, value); }
    void visitFloat(double& value) override { reflections.top()->SetDouble(messages.top(), fieldDescriptor, value); }
    void visitBool(bool& value) override { reflections.top()->SetBool(messages.top(), fieldDescriptor, value); }
    void visitString(std::string& value) override { reflections.top()->SetString(messages.top(), fieldDescriptor, value); }

private:
    const google::protobuf::FieldDescriptor* fieldDescriptor{};
    std::stack<google::protobuf::Message*> messages;
    std::stack<const google::protobuf::Reflection*> reflections;
    std::stack<const google::protobuf::Descriptor*> descriptors;
};
}
