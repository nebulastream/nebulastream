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

#ifndef NES_COMPAREOPERATION_HPP
#define NES_COMPAREOPERATION_HPP

#include <Experimental/NESIR/Operations/Operation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
class CompareOperation : public Operation {
  public:
    enum Comparator{
      // Comparator < 6 is signed int.
      IEQ = 0,
      INE = 1,
      ISLT = 2,
      ISLE = 3,
      ISGT = 4,
      ISGE = 5,
      // Comparator (> 5 && < 10) is unsigned int.
      IULT = 6,
      IULE = 7,
      IUGT = 8,
      IUGE = 9,
      // Comparator > 9 is float.
      FOLT = 10,
      FOLE = 11,
      FOGT = 12,
      FOGE = 13,
      FOEQ = 14,
      FONE = 15
    };
    CompareOperation(std::string identifier, std::string leftArgName, std::string rightArgName, Comparator comparator);
    ~CompareOperation() override = default;

    std::string getIdentifier();
    std::string getLeftArgName();
    std::string getRightArgName();
    Comparator getComparator();

    std::string toString() override;
  private:
    std::string identifier;
    std::string leftArgName;
    std::string rightArgName;
    Comparator comparator;
};
}// namespace NES
#endif//NES_COMPAREOPERATION_HPP
