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

#include <Util/PluginRegistry.hpp>

namespace NES
{

class DataType;
class LogicalFunction;

class LogicalFunction
{
public:
    LogicalFunction() = default;

    /// @brief infers the stamp for the logical function
    /// @param inputStamps of the arguments
    /// @return std::shared_ptr<DataType>
    [[nodiscard]] virtual std::shared_ptr<DataType> inferStamp(const std::vector<std::shared_ptr<DataType>>& inputStamps) const = 0;
    virtual ~LogicalFunction() = default;
};

class LogicalFunction : public LogicalFunction
{
public:
    [[nodiscard]] std::shared_ptr<DataType> inferStamp(const std::vector<std::shared_ptr<DataType>>& inputStamps) const final;
    [[nodiscard]] virtual std::shared_ptr<DataType> inferUnary(const std::shared_ptr<DataType>& input) const = 0;
};

class LogicalFunction : public LogicalFunction
{
public:
    [[nodiscard]] std::shared_ptr<DataType> inferStamp(const std::vector<std::shared_ptr<DataType>>& inputStamps) const final;
    [[nodiscard]] virtual std::shared_ptr<DataType>
    inferBinary(const std::shared_ptr<DataType>& left, const std::shared_ptr<DataType>& right) const = 0;
};

}
