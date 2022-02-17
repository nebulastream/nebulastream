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

#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_EVENTTIMEWINDOW_PHYSICALKEYEDGLOBALSLICESTOREAPPENDOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_EVENTTIMEWINDOW_PHYSICALKEYEDGLOBALSLICESTOREAPPENDOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedEventTimeWindowHandler.hpp>
#include <memory>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Appends the final slices to the global slice store.
 */
class PhysicalKeyedGlobalSliceStoreAppendOperator : public PhysicalUnaryOperator , public AbstractEmitOperator {
  public:
    PhysicalKeyedGlobalSliceStoreAppendOperator(
        OperatorId id,
        SchemaPtr inputSchema,
        SchemaPtr outputSchema,
        std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler);

    static std::shared_ptr<PhysicalKeyedGlobalSliceStoreAppendOperator>
    create(SchemaPtr inputSchema,
           SchemaPtr outputSchema,
           std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler) {
        return std::make_shared<PhysicalKeyedGlobalSliceStoreAppendOperator>(Util::getNextOperatorId(),
                                                                 inputSchema,
                                                                 outputSchema,
                                                                 keyedEventTimeWindowHandler);
    }

    std::string toString() const override;
    OperatorNodePtr copy() override;

    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> getWindowHandler() {
        return keyedEventTimeWindowHandler;
    }

  private:
    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler;
};

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_EVENTTIMEWINDOW_PHYSICALKEYEDGLOBALSLICESTOREAPPENDOPERATOR_HPP_
