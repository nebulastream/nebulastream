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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONFUNCTION_HPP
#define NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONFUNCTION_HPP
#include <Common/DataTypes/DataType.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Aggregation {
/**
 * This class is the Nautilus aggregation interface
 */
class AggregationFunction {
  public:
    AggregationFunction(DataTypePtr inputType, DataTypePtr finalType);

    /**
     * @brief lift adds the incoming value to the existing aggregation value
     * @param memref existing aggregation value
     * @param value the value to add
     */
    virtual void lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<>) = 0;
    /**
     * @brief combine composes to aggregation value into one
     * @param memref1 an aggregation value (intermediate result)
     * @param memref2 another aggregation value (intermediate result)
     */
    virtual void combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memre2) = 0;
    /**
     * @brief lower returns the aggregation value
     * @param memref the derived aggregation value
     */
    virtual Nautilus::Value<> lower(Nautilus::Value<Nautilus::MemRef> memref) = 0;
    /**
     * @brief resets the stored aggregation value to init (=0)
     * @param memref the current aggragtion value which need to be reset
     */
    virtual void reset(Nautilus::Value<Nautilus::MemRef> memref) = 0;

    virtual ~AggregationFunction();

  private:
    DataTypePtr inputType;
    DataTypePtr finalType;
};

using AggregationFunctionPtr = std::shared_ptr<AggregationFunction>;
}// namespace NES::Runtime::Execution::Aggregation

#endif//NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONFUNCTION_HPP
