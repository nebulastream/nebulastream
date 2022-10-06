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
#ifndef NES_NES_EXECUTION_ENGINE_SRC_EXPERIMENTAL_BABELFISH_IRSERIALIZATION_HPP_
#define NES_NES_EXECUTION_ENGINE_SRC_EXPERIMENTAL_BABELFISH_IRSERIALIZATION_HPP_
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <cpprest/json.h>
#include <set>
#include <map>

namespace NES::Nautilus {

class IRGraphvizSerialization {
  public:
    std::string serialize(std::shared_ptr<IR::IRGraph> ir);
    void serializeBlock(std::shared_ptr<IR::BasicBlock> block);
    std::string serializeOperation(std::shared_ptr<IR::Operations::Operation> block,
                                   std::string blockid,
                                   std::stringstream&,
                                   int operationIndex);
    std::string serializeOperation(IR::Operations::BasicBlockInvocation& blockInvocation);

  private:
    // std::stringstream ss;
    std::vector<std::string> blocks;
    std::set<std::string> visitedBlocks;
    std::map<std::string, std::string> idMap;
};

}// namespace NES::Nautilus

#endif//NES_NES_EXECUTION_ENGINE_SRC_EXPERIMENTAL_BABELFISH_IRSERIALIZATION_HPP_
