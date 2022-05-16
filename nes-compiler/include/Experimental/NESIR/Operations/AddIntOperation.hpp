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

#ifndef NES_ADDINTOPERATION_HPP
#define NES_ADDINTOPERATION_HPP

#include <Experimental/NESIR/Operations/Operation.hpp>

namespace NES {

//Todo: Decide: Leave type
class AddIntOperation : public Operation {
  public:
    AddIntOperation(std::string identifier, std::string leftArgName, std::string rightArgName);
    ~AddIntOperation() override = default;

    std::string getIdentifier();
    std::string getLeftArgName();
    std::string getRightArgName();
    bool classof(const Operation* Op);

  private:
    std::string identifier;
    std::string leftArgName;
    std::string rightArgName;
};
}// namespace NES
#endif//NES_ADDINTOPERATION_HPP
