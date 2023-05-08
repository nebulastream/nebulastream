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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BABELFISHLOWERINGPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BABELFISHLOWERINGPROVIDER_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/AddressOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
#include <Nautilus/IR/Operations/ConstBooleanOperation.hpp>
#include <Nautilus/IR/Operations/ConstFloatOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>
#include <Nautilus/IR/Types/Stamp.hpp>
#include <Nautilus/Util/Frame.hpp>
#include <nlohmann/json_fwd.hpp>
#include <sstream>
#include <unordered_set>
#include <vector>

namespace NES::Nautilus::Backends::Babelfish {

/**
 * @brief The lowering provider translates the IR to the BABELFISH code.
 */
class BabelfishLoweringProvider {
  public:
    BabelfishLoweringProvider() = default;
    static std::string lower(std::shared_ptr<IR::IRGraph> ir);

  private:
    using RegisterFrame = Frame<std::string, std::string>;
    using Code = std::stringstream;

    class LoweringContext {
      public:
        explicit LoweringContext(std::shared_ptr<IR::IRGraph> ir);
        Code process();

      private:
        std::vector<nlohmann::json> blocks;
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_map<std::string, std::string> activeBlocks;
        std::string process(const std::shared_ptr<IR::BasicBlock>&, RegisterFrame& frame);
        nlohmann::json process(IR::Operations::BasicBlockInvocation& opt);
        void process(const std::shared_ptr<IR::Operations::CompareOperation>& operation, nlohmann::json& opJson);
        void
        process(const std::shared_ptr<IR::Operations::Operation>& operation, short, std::vector<nlohmann::json>& block, RegisterFrame& frame);
    };
};
}// namespace NES::Nautilus::Backends::Babelfish
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BABELFISHLOWERINGPROVIDER_HPP_
