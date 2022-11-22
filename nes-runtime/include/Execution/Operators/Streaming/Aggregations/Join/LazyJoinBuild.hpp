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
#ifndef NES_LAZYJOINBUILD_HPP
#define NES_LAZYJOINBUILD_HPP

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinOperatorHandler.hpp>


namespace NES::Runtime::Execution::Operators {

class LazyJoinBuild : public ExecutableOperator {

  public:
    // TODO think about if this is the correct way to do this
    static constexpr auto NUM_PARTITIONS = 8 * 1024;

    // To use bitwise-and instead of a modulo, the mask should be (2^n) - 1
    static constexpr auto MASK = NUM_PARTITIONS - 1;


    /**
     * @brief builds a hash table with the record
     * @param ctx
     * @param record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;



  private:
    std::string joinFieldName;
    uint64_t handlerIndex;
    bool isLeftSide;
};


}
#endif//NES_LAZYJOINBUILD_HPP
