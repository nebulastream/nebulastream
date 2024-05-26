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

#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution {
    std::list<std::shared_ptr<StreamSliceInterface>> OperatorHandler::getStateToMigrate(uint64_t startTS, uint64_t stopTS) {
        NES_DEBUG("Get state to migrate from:", startTS, " to: ", stopTS);
        return {};
    };

    void OperatorHandler::restoreState(std::list<std::shared_ptr<StreamSliceInterface>> slices) {
        NES_DEBUG("Restore state for: ", slices.size(), " slices.");
    };
}