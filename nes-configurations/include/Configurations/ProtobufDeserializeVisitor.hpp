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
#include <limits>
#include <stack>
#include <stdexcept>
#include <string>
#include <string_view>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ISequenceOption.hpp>
#include <Configurations/WritingVisitor.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>
#include <sys/types.h>
#include <ErrorHandling.hpp>

namespace NES::Configurations
{

/// Traverses a configuration and overwrites its options with the values from the given message.
/// The message is expected to be a protobuf message that was filled with values by ProtobufSerializeOptionVisitor,
/// the focus in this class is only on overwriting the config values.
/// Uses protobuf reflection util to dynamically retrieve the values within the message.
class ProtobufDeserializeVisitor : public NES::Configurations::WritingVisitor
{
public:
    explicit ProtobufDeserializeVisitor(const google::protobuf::Message* message)
    {
        const google::protobuf::Descriptor* descriptor = message->GetDescriptor();
        reflections.push(message->GetReflection());
        messages.push(message);
        descriptors.push(descriptor);
    }

    void push(BaseOption& option) override
    {
        if (const auto* descriptor = descriptors.top()->FindFieldByName(option.getName()))
        {
            const auto* message = &reflections.top()->GetMessage(*messages.top(), descriptor);
            const auto* reflection = message->GetReflection();
            reflections.push(reflection);
            messages.push(message);
            descriptors.push(message->GetDescriptor());

            if (auto* sequenceOption = dynamic_cast<ISequenceOption*>(&option))
            {
                for (size_t i = 0; i < sequenceOption->size(); ++i)
                {
                    push(sequenceOption->operator[](i));
                    sequenceOption->operator[](i).accept(*this);
                    pop(sequenceOption->operator[](i));
                }
            }
            return;
        }
        throw InvalidConfigParameter("Field not found: {}", option.getName());
    }

    void pop(BaseOption&) override
    {
        reflections.pop();
        messages.pop();
        descriptors.pop();
    }

protected:
    void visitLeaf(BaseOption& option) override { fieldDescriptor = descriptors.top()->FindFieldByName(option.getName()); }
    void visitEnum(std::string_view, size_t& value) override
    {
        const uint64_t temp = reflections.top()->GetUInt64(*messages.top(), fieldDescriptor);
        if (temp > std::numeric_limits<size_t>::max())
        {
            throw std::overflow_error("Value exceeds size_t limits");
        }
        value = static_cast<size_t>(temp);
    }
    void visitUnsignedInteger(size_t& value) override { value = reflections.top()->GetUInt64(*messages.top(), fieldDescriptor); }
    void visitSignedInteger(ssize_t& value) override { value = reflections.top()->GetInt64(*messages.top(), fieldDescriptor); }
    void visitDouble(double& value) override { value = reflections.top()->GetDouble(*messages.top(), fieldDescriptor); }
    void visitBool(bool& value) override { value = reflections.top()->GetBool(*messages.top(), fieldDescriptor); }
    void visitString(std::string& value) override { value = reflections.top()->GetString(*messages.top(), fieldDescriptor); }

private:
    const google::protobuf::FieldDescriptor* fieldDescriptor{};
    std::stack<const google::protobuf::Message*> messages;
    std::stack<const google::protobuf::Reflection*> reflections;
    std::stack<const google::protobuf::Descriptor*> descriptors;
};

}
