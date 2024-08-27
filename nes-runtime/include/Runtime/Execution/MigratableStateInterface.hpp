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

#pragma once
#include <memory>
#include <vector>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution
{
/**
 * @brief Interface that defines operations to migrate an operator state
 */
class MigratableStateInterface
{
    /**
     * @brief Gets the state
     * @param startTS
     * @param stopTS
     * @return list of TupleBuffers
     */
    virtual std::vector<Memory::TupleBuffer> getStateToMigrate(uint64_t, uint64_t) = 0;

    /**
     * @brief Merges migrated slices
     * @param buffers
     */
    virtual void restoreState(std::vector<Memory::TupleBuffer>&) = 0;
};
} /// namespace NES::Runtime::Execution
