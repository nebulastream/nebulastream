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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONFUNCTION_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONFUNCTION_HPP_
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>


namespace NES::Runtime::Execution::Aggregation {
/**
 * This class is the Nautilus aggregation interface
 */
class AggregationFunction {
  public:
    AggregationFunction(PhysicalTypePtr inputType,
                        PhysicalTypePtr resultType,
                        Expressions::ExpressionPtr inputExpression,
                        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier);

    /**
     * @brief lift adds the incoming value to the existing aggregation value
     * @param memRef existing aggregation value
     * @param value the value to add
     */
    virtual void lift(Nautilus::MemRefVal memRef, Nautilus::Record& record) = 0;

    /**
     * @brief combine composes to aggregation value into one
     * @param memRef1 an aggregation value (intermediate result)
     * @param memRef2 another aggregation value (intermediate result)
     */
    virtual void combine(Nautilus::MemRefVal memRef1, Nautilus::MemRefVal memRef2) = 0;

    /**
     * @brief lower returns the aggregation value
     * @param memRef the derived aggregation value
     */
    virtual void lower(Nautilus::MemRefVal memRef, Nautilus::Record& resultRecord) = 0;

    /**
     * @brief resets the stored aggregation value to init (=0)
     * @param memRef the current aggragtion value which need to be reset
     */
    virtual void reset(Nautilus::MemRefVal memRef) = 0;

    /**
     * @brief gets the size of the partial aggregate in byte.
     * @return uint64_t
     */
    virtual uint64_t getSize() = 0;

    virtual ~AggregationFunction();

  protected:
    const PhysicalTypePtr inputType;
    const PhysicalTypePtr resultType;
    const Expressions::ExpressionPtr inputExpression;
    const Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier;

    /**
     * @brief Load a value from a memRef as a specific pyhsical type
     * @param memRef the memRef to load from
     * @param physicalType the intended data type to which the value should be casted
     * @return value in the type of physicalType
     */
    static Nautilus::VarVal loadFromMemRef(Nautilus::MemRefVal memRef, const PhysicalTypePtr& physicalType);

    static void storeToMemRef(Nautilus::MemRefVal memRef, const Nautilus::VarVal& execValue, const PhysicalTypePtr& physicalType);

    static Nautilus::VarVal createConstValue(int64_t value, const PhysicalTypePtr& physicalTypePtr);

    static Nautilus::VarVal createMinValue(const PhysicalTypePtr& physicalTypePtr);

    static Nautilus::VarVal createMaxValue(const PhysicalTypePtr& physicalTypePtr);
};

using AggregationFunctionPtr = std::shared_ptr<AggregationFunction>;
}// namespace NES::Runtime::Execution::Aggregation

#endif// NES_EXECUTION_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONFUNCTION_HPP_
