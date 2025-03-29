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

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>

namespace NES
{
/// @brief A RenameLogicalFunction allows us to rename an attribute value via .as in the query
class RenameLogicalFunction : public LogicalFunction
{
public:
    static std::shared_ptr<LogicalFunction> create(std::shared_ptr<FieldAccessLogicalFunction> originalField, std::string newFieldName);

    [[nodiscard]] bool equal(std::shared_ptr<LogicalFunction> const& rhs) const override;


    std::string getNewFieldName() const;

    void inferStamp(const Schema& schema) override;


    std::shared_ptr<FieldAccessLogicalFunction> getOriginalField() const;

    std::shared_ptr<LogicalFunction> clone() const override;

protected:
    explicit RenameLogicalFunction(const std::shared_ptr<RenameLogicalFunction> other);

    [[nodiscard]] std::string toString() const override;

private:
    RenameLogicalFunction(const std::shared_ptr<FieldAccessLogicalFunction>& originalField, std::string newFieldName);

    std::shared_ptr<FieldAccessLogicalFunction> originalField;
    std::string newFieldName;
};

}
