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
#include <Synopses/AbstractSynopsis.hpp>

namespace NES::Runtime::Execution::Operators {

class SynopsesOperator : public ExecutableOperator {

    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    uint64_t handlerIndex;
    ASP::AbstractSynopsesPtr synopses;
};

}// namespace NES::Runtime::Execution::Operators

#endif//NES_SYNOPSESOPERATOR_HPP
