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

#ifndef NES_LOADOPERATION_HPP
#define NES_LOADOPERATION_HPP

#include <Experimental/NESIR/Operations/Operation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
class LoadOperation : public Operation {
  public:
    explicit LoadOperation(std::string identifier, std::string argName);
    ~LoadOperation() override = default;

    std::string getIdentifier();
    std::string getArgName();

    static bool classof(const Operation *Op);
  private:
    std::string identifier;
    std::string argName;
};
}// namespace NES
#endif//NES_LOADOPERATION_HPP
