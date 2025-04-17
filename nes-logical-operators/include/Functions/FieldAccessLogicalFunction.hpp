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

/// @brief A FieldAccessFunction reads a specific field of the current record.
/// It can be created typed or untyped.
class FieldAccessLogicalFunction : public LogicalFunction
{
public:
    static std::shared_ptr<LogicalFunction> create(std::shared_ptr<DataType> stamp, std::string fieldName);
    static std::shared_ptr<LogicalFunction> create(std::string fieldName);

    bool equal(const std::shared_ptr<LogicalFunction>& rhs) const override;

    std::string getFieldName() const;
    void updateFieldName(std::string fieldName);

    void inferStamp(const Schema& schema) override;

protected:
    explicit FieldAccessLogicalFunction(FieldAccessLogicalFunction* other);

    FieldAccessLogicalFunction(std::shared_ptr<DataType> stamp, std::string fieldName);

    std::string toString() const override;

    std::string fieldName;
};

}
