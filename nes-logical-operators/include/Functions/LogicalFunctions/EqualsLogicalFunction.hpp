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
#include <Abstract/LogicalFunction.hpp>

namespace NES
{

class EqualsLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "Equals";

    EqualsLogicalFunction(LogicalFunction left, LogicalFunction right);
    EqualsLogicalFunction(const EqualsLogicalFunction& other);
    ~EqualsLogicalFunction() override = default;

    [[nodiscard]] SerializableFunction serialize() const override;

    bool validateBeforeLowering() const;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const;

    const DataType& getStamp() const override { return *stamp; };
    void setStamp(std::shared_ptr<DataType> stamp) override { this->stamp = stamp; };
    bool inferStamp(Schema) override { return false; };
    std::vector<LogicalFunction> getChildren()  const override { return {left, right}; };
    void setChildren(std::vector<LogicalFunction> children)  override { left = children[0]; right = children[1]; };
    std::string getType() const override { return std::string(NAME); }
    [[nodiscard]] std::string toString() const override;

private:
    LogicalFunction left, right;
    std::shared_ptr<DataType> stamp;
};
}
FMT_OSTREAM(NES::EqualsLogicalFunction);
