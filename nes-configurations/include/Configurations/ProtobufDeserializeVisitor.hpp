#include <stack>
#include <Configurations/WritingVisitor.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/util/json_util.h>

namespace NES::Configurations
{

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
            if (auto* sequenceOption = dynamic_cast<SequenceOption<StringOption>*>(&option))
            {

                const auto* message = &reflections.top()->GetMessage(*messages.top(), descriptor);
                const auto* reflection = message->GetReflection();
                reflections.push(reflection);
                messages.push(message);
                descriptors.push(message->GetDescriptor());

                for (size_t i = 0; i < sequenceOption->size(); ++i)
                {
                    push(sequenceOption->operator[](i));
                    sequenceOption->operator[](i).accept(*this);
                    pop(sequenceOption->operator[](i));
                }
            }
            else if (auto* sequenceOption = dynamic_cast<SequenceOption<FloatOption>*>(&option))
            {

                const auto* message = &reflections.top()->GetMessage(*messages.top(), descriptor);
                const auto* reflection = message->GetReflection();
                reflections.push(reflection);
                messages.push(message);
                descriptors.push(message->GetDescriptor());

                for (size_t i = 0; i < sequenceOption->size(); ++i)
                {
                    push(sequenceOption->operator[](i));
                    sequenceOption->operator[](i).accept(*this);
                    pop(sequenceOption->operator[](i));
                }
            }
            else if (auto* sequenceOption = dynamic_cast<SequenceOption<BoolOption>*>(&option))
            {

                const auto* message = &reflections.top()->GetMessage(*messages.top(), descriptor);
                const auto* reflection = message->GetReflection();
                reflections.push(reflection);
                messages.push(message);
                descriptors.push(message->GetDescriptor());

                for (size_t i = 0; i < sequenceOption->size(); ++i)
                {
                    push(sequenceOption->operator[](i));
                    sequenceOption->operator[](i).accept(*this);
                    pop(sequenceOption->operator[](i));
                }
            }
            else if (auto* sequenceOption = dynamic_cast<SequenceOption<UIntOption>*>(&option))
            {

                const auto* message = &reflections.top()->GetMessage(*messages.top(), descriptor);
                const auto* reflection = message->GetReflection();
                reflections.push(reflection);
                messages.push(message);
                descriptors.push(message->GetDescriptor());

                for (size_t i = 0; i < sequenceOption->size(); ++i)
                {
                    push( sequenceOption->operator[](i));
                    sequenceOption->operator[](i).accept(*this);
                    pop( sequenceOption->operator[](i));
                }
            }
            else
            {
                const auto* message = &reflections.top()->GetMessage(*messages.top(), descriptor);
                const auto* reflection = message->GetReflection();
                reflections.push(reflection);
                messages.push(message);
                descriptors.push(message->GetDescriptor());
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
    void visitEnum(std::string_view, size_t& underlying) override
    {
        underlying = reflections.top()->GetUInt64(*messages.top(), fieldDescriptor);
    }
    void visitUnsignedInteger(size_t& value) override { value = reflections.top()->GetUInt64(*messages.top(), fieldDescriptor); }
    void visitSignedInteger(ssize_t& value) override { value = reflections.top()->GetInt64(*messages.top(), fieldDescriptor); }
    void visitFloat(double& value) override { value = reflections.top()->GetDouble(*messages.top(), fieldDescriptor); }
    void visitBool(bool& value) override { value = reflections.top()->GetBool(*messages.top(), fieldDescriptor); }
    void visitString(std::string& value) override { value = reflections.top()->GetString(*messages.top(), fieldDescriptor); }

private:
    const google::protobuf::FieldDescriptor* fieldDescriptor{};
    std::stack<const google::protobuf::Message*> messages;
    std::stack<const google::protobuf::Reflection*> reflections;
    std::stack<const google::protobuf::Descriptor*> descriptors;
};

}