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

std::vector<Runtime::TupleBuffer> OperatorHandler::serializeOperatorHandlerForMigration() {
    NES_WARNING("No state inside abstract operator handler");
    return {};
}
std::vector<Runtime::TupleBuffer> OperatorHandler::getStateToMigrate(uint64_t, uint64_t) {
    NES_WARNING("No state inside abstract operator handler");
    return {};
};

void OperatorHandler::restoreState(std::vector<Runtime::TupleBuffer>&) {
    NES_WARNING("Restore state is not implemented in abstract operator handler.");
};

void OperatorHandler::restoreStateFromFile(std::ifstream&) {
    NES_WARNING("Restore state from the file is not implemented in abstract operator handler.");
};
}// namespace NES::Runtime::Execution
