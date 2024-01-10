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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_LOGICALOPERATIONS_COMPAREOPERATION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_LOGICALOPERATIONS_COMPAREOPERATION_HPP_

#include <Nautilus/IR/Operations/Operation.hpp>

namespace NES::Nautilus::IR::Operations {
class CompareOperation : public Operation {
  public:
    enum Comparator {
        EQ = 0,
        NE = 1,
        LT = 2,
        LE = 3,
        GT = 4,
        GE = 5,
    };
    CompareOperation(OperationIdentifier identifier, OperationPtr leftInput, OperationPtr rightInput, Comparator comparator);
    ~CompareOperation() override = default;

    OperationPtr getLeftInput();
    OperationPtr getRightInput();
    void setLeftInput(OperationWPtr newLeftInput);
    void setRightInput(OperationWPtr newRightInput);
    Comparator getComparator();
    bool isLessThan();
    bool isLessEqual();
    bool isGreaterThan();
    bool isGreaterEqual();
    bool isEquals();
    bool isLessThanOrGreaterThan();
    bool isLess();
    bool isGreater();

    std::string getComparatorAsString();

    std::string toString() override;

  private:
    OperationWPtr leftInput;
    OperationWPtr rightInput;
    Comparator comparator;
};
}// namespace NES::Nautilus::IR::Operations
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_LOGICALOPERATIONS_COMPAREOPERATION_HPP_
