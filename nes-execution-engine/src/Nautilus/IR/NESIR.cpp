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

#include <Nautilus/IR/NESIR.hpp>
#include <sstream>
#include <utility>
#include <Experimental/Utility/NESIRDumpHandler.hpp>

namespace NES::Nautilus::IR {

std::shared_ptr<Operations::FunctionOperation> NESIR::addRootOperation(std::shared_ptr<Operations::FunctionOperation> rootOperation) {
    this->rootOperation = std::move(rootOperation);
    return this->rootOperation;
}

std::shared_ptr<Operations::FunctionOperation> NESIR::getRootOperation() {
    return rootOperation;
}

bool NESIR::getIsSCF() { return isSCF; }
void NESIR::setIsSCF(bool isSCF) { this->isSCF = isSCF; }

std::string NESIR::toString() {
    std::stringstream ss;
    ss << "NESIR {\n";
    auto dumpHandler = Nautilus::IR::NESIRDumpHandler::create(ss);
    dumpHandler->dump(rootOperation);
    ss << "} //NESIR";
    return ss.str();
}

}// namespace NES
