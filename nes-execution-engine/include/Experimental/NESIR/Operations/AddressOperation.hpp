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

#ifndef NES_ADDRESSOPERATION_HPP
#define NES_ADDRESSOPERATION_HPP

#include <Experimental/NESIR/Operations/Operation.hpp>
#include <vector>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
class AddressOperation : public Operation {
public:
    explicit AddressOperation(std::string identifier,
                            PrimitiveStamp dataType, uint64_t recordWidthInBytes,
                              uint64_t fieldOffsetInBytes, std::string recordIdxName, std::string addressSourceName);
    ~AddressOperation() override = default;

    std::string getIdentifier();
    PrimitiveStamp getDataType();
    uint64_t getRecordWidthInBytes();
    uint64_t getFieldOffsetInBytes();
    std::string getRecordIdxName();
    std::string getAddressSourceName();

    std::string toString() override;
    static bool classof(const Operation *Op);
private:
    std::string identifier;
    PrimitiveStamp dataType;
    uint64_t recordWidth;
    uint64_t fieldOffset;
    std::string recordIdxName;
    std::string addressSourceName;
};
}// namespace NES

#endif//NES_ADDRESSOPERATION_HPP
