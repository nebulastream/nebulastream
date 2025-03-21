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

#include <Abstract/LogicalFunction.hpp>

namespace NES
{
class OrLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "Or";

    OrLogicalFunction(LogicalFunction left, LogicalFunction right);
    OrLogicalFunction(const OrLogicalFunction& other);

    bool validateBeforeLowering() const;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const;
    void inferStamp(const Schema& schema);

    const DataType& getStamp() const override { return *stamp; };
    void setStamp(std::shared_ptr<DataType> stamp) override { this->stamp = stamp; };
    std::vector<LogicalFunction> getChildren()  const override { return {left, right}; };
    std::string getType() const override { return std::string(NAME); }
    [[nodiscard]] std::string toString() const override;

private:
    std::shared_ptr<DataType> stamp;
    LogicalFunction left;
    LogicalFunction right;
};
}
FMT_OSTREAM(NES::OrLogicalFunction);

