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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_ADDRESSOPERATION_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_ADDRESSOPERATION_HPP_

#include <Nautilus/IR/Operations/Operation.hpp>
#include <vector>

namespace NES::Nautilus::IR::Operations {
class AddressOperation : public Operation {
  public:
    explicit AddressOperation(OperationIdentifier identifier,
                              PrimitiveStamp dataType,
                              uint64_t recordWidthInBytes,
                              uint64_t fieldOffsetInBytes,
                              std::string recordIdxName,
                              std::string addressSourceName);
    ~AddressOperation() override = default;

    PrimitiveStamp getDataType();
    uint64_t getRecordWidthInBytes();
    uint64_t getFieldOffsetInBytes();
    std::string getRecordIdxName();
    std::string getAddressSourceName();

    std::string toString() override;
    static bool classof(const Operation* Op);

  private:
    PrimitiveStamp dataType;
    uint64_t recordWidth;
    uint64_t fieldOffset;
    std::string recordIdxName;
    std::string addressSourceName;
};
}// namespace NES::Nautilus::IR::Operations

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_ADDRESSOPERATION_HPP_
