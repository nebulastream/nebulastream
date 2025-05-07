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

#include <memory>

#include <Functions/NodeFunction.hpp>
#include <LogicalFunctionRegistry.hpp>
#include "Common/DataTypes/Integer.hpp"
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Float.hpp>

namespace NES
{

class SpeechRecognitionLabelLogicalFunction final : public NodeFunction
{
public:
    SpeechRecognitionLabelLogicalFunction() : NodeFunction(DataTypeProvider::provideDataType("VARSIZED"), "SpeechRecognitionLabel") { }

    void setChildren(std::shared_ptr<NodeFunction> argmax) { addChildWithEqual(argmax); }

    ~SpeechRecognitionLabelLogicalFunction() noexcept override = default;
    bool validateBeforeLowering() const override
    {
        return children.size() == 1 && Util::instanceOf<Integer>(Util::as_if<NodeFunction>(children.at(0))->getStamp());
    }
    std::shared_ptr<NodeFunction> deepCopy() override
    {
        auto copy = std::make_shared<SpeechRecognitionLabelLogicalFunction>();
        copy->setChildren(Util::as<NodeFunction>(children.at(0)));
        return copy;
    }
    bool equal(const std::shared_ptr<Node>& rhs) const override
    {
        if (auto other = std::dynamic_pointer_cast<SpeechRecognitionLabelLogicalFunction>(rhs))
        {
            return std::ranges::equal(children, other->children, [](const auto& lhs, const auto& rhs) { return lhs->equal(rhs); });
        }
        return false;
    }
    bool isTriviallySerializable() override { return true; }
    void inferStamp(const Schema& schema) override
    {
        if (children.size() != 1)
        {
            throw TypeInferenceException("Function Expects 1 Argument");
        }
        auto child = Util::as_if<NodeFunction>(children[0]);
        child->inferStamp(schema);

        if (!NES::Util::instanceOf<Integer>(child->getStamp()))
        {
            throw TypeInferenceException("Function Expects 1 Argument with Integer input");
        }

        this->setStamp(DataTypeProvider::provideDataType("VARSIZED"));
    }

protected:
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override
    {
        fmt::println(os, "SPEECH_RECOGNITION_LABEL({})", *children.at(0));
        return os;
    }

    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override
    {
        fmt::println(os, "SPEECH_RECOGNITION_LABEL()");
        return os;
    }
};

}

namespace NES::LogicalFunctionGeneratedRegistrar
{
/// declaration of register functions for 'LogicalFunctions'
LogicalFunctionRegistryReturnType RegisterSpeechRecognitionLabelLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() != 1)
    {
        throw TypeInferenceException("Function Expects 1 Argument");
    }

    auto function = std::make_shared<SpeechRecognitionLabelLogicalFunction>();
    function->setChildren(arguments.childFunctions[0]);
    return function;
}
}
