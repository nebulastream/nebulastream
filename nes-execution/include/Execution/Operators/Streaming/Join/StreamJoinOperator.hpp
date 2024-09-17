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

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Util/Common.hpp>
#include <magic_enum.hpp>

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief This class acts as the parent class for our stream join. It stores the join strategy and the window strategy.
 * Furthermore, it provides a static method for casting from a void* to any given StreamJoinOperatorHandler.
 * #4184 investigates, if magic_enum is necessary here
 */
class StreamJoinOperator
{
public:
    StreamJoinOperator(QueryCompilation::StreamJoinStrategy joinStrategy);

    /**
     * @brief This method casts from a void* pointer depending on the join and window strategy to the correct derived class
     * and then pack to a parent class. This is necessary, as we do not always exactly know the child class.
     * @tparam OutputClass class to be casted to
     * @param ptrOpHandler
     * @param joinStrategyInt
     * @return OutputClass*
     */
    template <typename OutputClass = StreamJoinOperatorHandler>
    static OutputClass* getSpecificOperatorHandler(void* ptrOpHandler, uint64_t joinStrategyInt)
    {
        auto joinStrategy = magic_enum::enum_value<QueryCompilation::StreamJoinStrategy>(joinStrategyInt);
        switch (joinStrategy)
        {
            case QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED:
            case QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING:
            case QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE:
            case QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL: {
                auto* tmpOpHandler = static_cast<HJOperatorHandlerSlicing*>(ptrOpHandler);
                return dynamic_cast<OutputClass*>(tmpOpHandler);
            }
            case QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN: {
                auto* tmpOpHandler = static_cast<NLJOperatorHandlerSlicing*>(ptrOpHandler);
                return dynamic_cast<OutputClass*>(tmpOpHandler);
            }
        }
    }

    QueryCompilation::StreamJoinStrategy joinStrategy;
};
} /// namespace NES::Runtime::Execution::Operators
