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
#include <string>
#include <Functions/LogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

/// This function node represents a constant value and a fixed data type.
/// Thus the samp of this function is always fixed.
class ConstantValueLogicalFunction : public LogicalFunction
{
public:
    static std::shared_ptr<LogicalFunction> create(const std::shared_ptr<DataType>& type, std::string value);
    ~ConstantValueLogicalFunction() noexcept override = default;

    std::string getConstantValue() const;

    void inferStamp(const Schema& schema) override;

    [[nodiscard]] bool operator==(const std::shared_ptr<LogicalFunction>& rhs) const override;


protected:
    explicit ConstantValueLogicalFunction(const ConstantValueLogicalFunction* other);

    std::string toString() const override;

private:
    explicit ConstantValueLogicalFunction(const std::shared_ptr<DataType>& type, std::string&& value);
    std::string constantValue;
};
}
