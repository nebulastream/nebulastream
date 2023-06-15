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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_FUNCTIONS_LOGICALFUNCTIONREGISTRY_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_FUNCTIONS_LOGICALFUNCTIONREGISTRY_HPP_

#include <Util/PluginRegistry.hpp>
namespace NES {

class DataType;
class LogicalFunction;
using DataTypePtr = std::shared_ptr<DataType>;

/**
 * @brief The LogicalFunctionRegistry manages all logical functions.
 */
using LogicalFunctionRegistry = Util::PluginFactory<LogicalFunction>;

/**
 * @brief Base class for all logical functions.
 */
class LogicalFunction {
  public:
    LogicalFunction() = default;
    /**
     * @brief infers the stamp for the logical function
     * @param inputStamps of the arguments
     * @return DataTypePtr
     */
    [[nodiscard]] virtual DataTypePtr inferStamp(const std::vector<DataTypePtr>& inputStamps) const = 0;
    virtual ~LogicalFunction() = default;
};

class UnaryLogicalFunction : public LogicalFunction {
  public:
    [[nodiscard]] DataTypePtr inferStamp(const std::vector<DataTypePtr>& inputStamps) const final;
    [[nodiscard]] virtual DataTypePtr inferUnary(const DataTypePtr& input) const = 0;
};

class BinaryLogicalFunction : public LogicalFunction {
  public:
    [[nodiscard]] DataTypePtr inferStamp(const std::vector<DataTypePtr>& inputStamps) const final;
    [[nodiscard]] virtual DataTypePtr inferBinary(const DataTypePtr& left, const DataTypePtr& right) const = 0;
};

}// namespace NES
#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_FUNCTIONS_LOGICALFUNCTIONREGISTRY_HPP_
