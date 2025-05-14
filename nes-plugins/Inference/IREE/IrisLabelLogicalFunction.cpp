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

namespace NES
{

class IrisLabelLogicalFunction : public NodeFunction
{
public:
    IrisLabelLogicalFunction() : NodeFunction(DataTypeProvider::provideDataType("VARSIZED"), "iris_label") { }

    void
    setChildren(std::shared_ptr<NodeFunction> setosa, std::shared_ptr<NodeFunction> versicolor, std::shared_ptr<NodeFunction> virginica)
    {
        addChildWithEqual(setosa);
        addChildWithEqual(versicolor);
        addChildWithEqual(virginica);
    }

    ~IrisLabelLogicalFunction() noexcept override = default;
    bool validateBeforeLowering() const override
    {
        return std::ranges::all_of(
            children, [](const auto& child) { return Util::instanceOf<NES::Float>(Util::as<NodeFunction>(child)->getStamp()); });
    }
    std::shared_ptr<NodeFunction> deepCopy() override
    {
        auto copy = std::make_shared<IrisLabelLogicalFunction>();
        copy->setChildren(
            Util::as<NodeFunction>(children.at(0)), Util::as<NodeFunction>(children.at(1)), Util::as<NodeFunction>(children.at(2)));
        return copy;
    }
    bool equal(const std::shared_ptr<Node>& rhs) const override
    {
        if (auto other = std::dynamic_pointer_cast<IrisLabelLogicalFunction>(rhs))
        {
            return std::ranges::equal(children, other->children, [](const auto& lhs, const auto& rhs) { return lhs->equal(rhs); });
        }
        return false;
    }

    void inferStamp(const Schema& schema) override
    {
        std::ranges::for_each(children, [&](const auto& child) { Util::as<NodeFunction>(child)->inferStamp(schema); });

        const auto allFloat = std::ranges::all_of(
            children, [](const auto& child) { return Util::instanceOf<NES::Float>(Util::as<NodeFunction>(child)->getStamp()); });

        if (allFloat)
        {
            this->setStamp(DataTypeProvider::provideDataType("VARSIZED"));
        }
        else
        {
            throw TypeInferenceException("Expected all three parameters to be floating types");
        }
    }

protected:
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override
    {
        fmt::println(os, "IRIS_LABEL({},{},{})", *children.at(0), *children.at(1), *children.at(2));
        return os;
    }

    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override
    {
        fmt::println(os, "IRIS_LABEL()");
        return os;
    }
};

}

namespace NES::LogicalFunctionGeneratedRegistrar
{
/// declaration of register functions for 'LogicalFunctions'
LogicalFunctionRegistryReturnType Registeriris_labelLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() != 3)
    {
        throw TypeInferenceException("Function Expects 3 Arguments");
    }

    auto function = std::make_shared<IrisLabelLogicalFunction>();
    function->setChildren(arguments.childFunctions[0], arguments.childFunctions[1], arguments.childFunctions[2]);
    return function;
}
}
