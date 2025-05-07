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
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES
{

class MaxValueLogicalFunction : public NodeFunction
{
public:
    MaxValueLogicalFunction() : NodeFunction(DataTypeProvider::provideDataType("UINT64"), "MaxValue") { }

    void setChildren(std::shared_ptr<NodeFunction> inputTensor) { addChildWithEqual(inputTensor); }

    ~MaxValueLogicalFunction() noexcept override = default;
    bool validateBeforeLowering() const override
    {
        if (children.size() != 1)
        {
            return false;
        }
        if (!Util::instanceOf<VariableSizedDataType>(Util::as_if<NodeFunction>(children[0])->getStamp()))
        {
            return false;
        }
        return true;
    }

    std::shared_ptr<NodeFunction> deepCopy() override
    {
        auto copy = std::make_shared<MaxValueLogicalFunction>();
        copy->setChildren(Util::as_if<NodeFunction>(children.at(0)));
        return copy;
    }

    bool equal(const std::shared_ptr<Node>& rhs) const override
    {
        if (auto other = std::dynamic_pointer_cast<MaxValueLogicalFunction>(rhs))
        {
            return std::ranges::equal(children, other->children, [](const auto& lhs, const auto& rhs) { return lhs->equal(rhs); });
        }
        return false;
    }

    void inferStamp(const Schema& schema) override
    {
        if (children.size() != 1)
        {
            throw TypeInferenceException("Function Expects 1 Argument");
        }

        Util::as_if<NodeFunction>(children.at(0))->inferStamp(schema);

        if (!Util::instanceOf<VariableSizedDataType>(Util::as_if<NodeFunction>(children[0])->getStamp()))
        {
            throw TypeInferenceException("Function Expects 1 Argument with VarSized input");
        }

        this->setStamp(DataTypeProvider::provideDataType("FLOAT32"));
    }

    bool isTriviallySerializable() override { return true; }

protected:
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override
    {
        fmt::println(os, "MAX_VALUE({})", *children.at(0));
        return os;
    }

    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override
    {
        fmt::println(os, "MAX_VALUE()");
        return os;
    }
};

}

namespace NES::LogicalFunctionGeneratedRegistrar
{
/// declaration of register functions for 'LogicalFunctions'
LogicalFunctionRegistryReturnType RegisterMaxValueLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() != 1)
    {
        throw TypeInferenceException("Function Expects 1 Argument");
    }

    auto function = std::make_shared<MaxValueLogicalFunction>();
    function->setChildren(arguments.childFunctions[0]);
    return function;
}
}
