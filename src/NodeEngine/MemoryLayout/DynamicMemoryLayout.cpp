/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <NodeEngine/MemoryLayout/DynamicMemoryLayout.hpp>

namespace NES::NodeEngine::DynamicMemoryLayout {

bool DynamicMemoryLayout::isCheckBoundaryFieldChecks() const { return checkBoundaryFieldChecks; }
uint64_t DynamicMemoryLayout::getRecordSize() const { return recordSize; }
const std::vector<FIELD_SIZE>& DynamicMemoryLayout::getFieldSizes() const { return fieldSizes; }

DynamicMemoryLayout::DynamicMemoryLayout(bool checkBoundaryFieldChecks, uint64_t recordSize, std::vector<FIELD_SIZE>& fieldSizes)
    : checkBoundaryFieldChecks(checkBoundaryFieldChecks), recordSize(recordSize), fieldSizes(fieldSizes) {}
DynamicMemoryLayout::DynamicMemoryLayout()
    : checkBoundaryFieldChecks(true), recordSize(0), fieldSizes(std::vector<FIELD_SIZE>()) {}

/**
 * @param fieldName
 * @return either field index for fieldName or empty optinal
 */
    std::optional<uint64_t> DynamicMemoryLayout::getFieldIndexFromName(std::string fieldName) const {
        auto nameFieldIt = nameFieldIndexMap.find(fieldName);
        if (nameFieldIt == nameFieldIndexMap.end()) {
            return std::nullopt;
        } else {
            return std::optional<uint64_t>(nameFieldIt->second);
        }
    }
}// namespace NES::NodeEngine::DynamicMemoryLayout
