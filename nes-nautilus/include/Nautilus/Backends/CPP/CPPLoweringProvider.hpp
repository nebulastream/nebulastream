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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPLOWERINGPROVIDER_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPLOWERINGPROVIDER_HPP_

#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/Util/Frame.hpp>

namespace NES::Nautilus::Backends::CPP {

/**
 * @brief The lowering provider translates the IR to the cpp code.
 */
class CPPLoweringProvider {
  public:
    CPPLoweringProvider() = default;
    static std::string lower(std::shared_ptr<IR::IRGraph> ir);
};
}// namespace NES::Nautilus::Backends::CPP
#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPLOWERINGPROVIDER_HPP_
