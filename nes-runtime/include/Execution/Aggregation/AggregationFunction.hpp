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

#ifndef NES_AGGREGATIONFUNCTION_HPP
#define NES_AGGREGATIONFUNCTION_HPP
#include <Common/DataTypes/DataType.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Aggregation {
/**
 * TODO 3250: doc
 */
class AggregationFunction {
  public:
    AggregationFunction(DataTypePtr inputType, DataTypePtr finalType);

    /**
     * TODO 3250: docs
     */
    virtual void lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<>) = 0;
    virtual void combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memre2) = 0;
    virtual Nautilus::Value<> lower(Nautilus::Value<Nautilus::MemRef> memref) = 0;
    virtual void reset(Nautilus::Value<Nautilus::MemRef> memref) = 0;

    virtual ~AggregationFunction();

  private:
    DataTypePtr inputType;
    DataTypePtr finalType;
};

using AggregationFunctionPtr = std::shared_ptr<AggregationFunction>;
}// namespace NES::Runtime::Execution::Aggregation

#endif//NES_AGGREGATIONFUNCTION_HPP
