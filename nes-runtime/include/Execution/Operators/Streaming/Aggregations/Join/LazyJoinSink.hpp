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

#ifndef NES_LAZYJOINSINK_HPP
#define NES_LAZYJOINSINK_HPP
#include <Execution/Operators/Operator.hpp>


namespace NES::Runtime::Execution::Operators {

class LazyJoinSink : public Operator {

  public:
    /**
     * @brief receives a record buffer and then performs the join for the corresponding bucket
     * @param executionCtx
     * @param recordBuffer
     */
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  private:
    /**
     * @brief performs the join by comparing buckets (vector of fixed pages)
     * @param executionContext
     * @param probeSide
     * @param buildSide
     */
    size_t executeJoin(ExecutionContext& executionContext, std::vector<FixedPage>&& probeSide,
                       std::vector<FixedPage>&& buildSide) const;


  private:
    uint64_t handlerIndex;
    std::string joinFieldName;

};


} //namespace NES::Runtime::Execution::Operators
#endif//NES_LAZYJOINSINK_HPP
