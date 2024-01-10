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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_LOOP_LOOPINFO_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_LOOP_LOOPINFO_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <vector>
namespace NES::Nautilus::IR::Operations {

class LoopInfo {
  public:
    virtual bool isCountedLoop() { return false; }
    virtual ~LoopInfo() = default;
};

class DefaultLoopInfo : public LoopInfo {};

class CountedLoopInfo : public LoopInfo {
  public:
    uint64_t lowerBound;
    uint64_t upperBound;
    uint64_t stepSize;
    bool isCountedLoop() override { return true; }
};

}// namespace NES::Nautilus::IR::Operations

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_LOOP_LOOPINFO_HPP_
