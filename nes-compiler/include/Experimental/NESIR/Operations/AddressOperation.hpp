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

namespace NES {
class AddressOperation : public Operation {
public:
    explicit AddressOperation(Operation::BasicType dataType, void* baseAddress, uint64_t widthOffset, uint64_t fieldOffset);
    ~AddressOperation() override = default;

    Operation::BasicType getDataType();

    void* getBaseAddress();
    uint64_t getWidthOffset();
    uint64_t getFieldOffset();

    static bool classof(const Operation *Op);
private:
    Operation::BasicType dataType;
    void* baseAddress;
    uint64_t widthOffset;
    uint64_t fieldOffset;
};
}// namespace NES

#endif//NES_ADDRESSOPERATION_HPP
