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

#ifndef NES_SYNOPSESOPERATOR_HPP
#define NES_SYNOPSESOPERATOR_HPP

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Experimental/Synopses/AbstractSynopsis.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Nautilus Operator for a specific synopsis. For now, it also takes care of windowing, but this can change later on
 */
class SynopsesOperator : public ExecutableOperator {

  public:
    /**
     * @brief Constructor for the nautilus operator corresponding to a synopses
     * @param synopses
     */
    explicit SynopsesOperator(uint64_t handlerIndex, const ASP::AbstractSynopsesPtr& synopses);

    /**
     * @brief Sets up the synopses and the operator, e.g., initializing the underlying synopsis
     * @param executionCtx
     */
    void setup(ExecutionContext& executionCtx) const override;

    void open(ExecutionContext &executionCtx, RecordBuffer &recordBuffer) const override;

    /**
     * @brief Passes the record to the synopsis so that it can see and act upon it
     * @param ctx
     * @param record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    uint64_t handlerIndex;
    ASP::AbstractSynopsesPtr synopses;
};

}// namespace NES::Runtime::Execution::Operators

#endif//NES_SYNOPSESOPERATOR_HPP
