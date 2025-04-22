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
#include <Functions/LogicalFunction.hpp>
namespace NES
{

/// @brief A case function has at least one when function and one default function.
/// All when functions are evaluated and the first one with a true condition is returned.
class CaseLogicalFunction : public LogicalFunction
{
public:
    explicit CaseLogicalFunction(std::shared_ptr<DataType> stamp);
    ~CaseLogicalFunction() noexcept override = default;

    static std::shared_ptr<LogicalFunction>
    create(const std::vector<std::shared_ptr<LogicalFunction>>& whenExps, const std::shared_ptr<LogicalFunction>& defaultExp);

    void setChildren(const std::vector<std::shared_ptr<LogicalFunction>>& whenExps, const std::shared_ptr<LogicalFunction>& defaultExp);

    /// @brief gets the vector of when children.
    std::vector<std::shared_ptr<LogicalFunction>> getWhenChildren() const;

    /// @brief gets the node representing the default child.
    std::shared_ptr<LogicalFunction> getDefaultExp() const;
    void inferStamp(std::shared_ptr<Schema> schema) override;

    [[nodiscard]] bool equal(const std::shared_ptr<LogicalFunction>& rhs) const final;


protected:
    explicit CaseLogicalFunction(CaseLogicalFunction* other);

    [[nodiscard]] std::string toString() const override;
};

}
