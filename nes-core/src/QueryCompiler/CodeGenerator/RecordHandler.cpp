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
#include <QueryCompiler/CodeGenerator/RecordHandler.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::QueryCompilation {
RecordHandlerPtr RecordHandler::create() { return std::make_shared<RecordHandler>(); }

void RecordHandler::registerAttribute(const std::string& name, ExpressionStatementPtr variableAccessStatement) {
    if (hasAttribute(name)) {
        NES_DEBUG("RecordHandler: replace attribute with name " << name);
    } else {
        NES_DEBUG("RecordHandler: place new attribute with name " << name);
    }
    this->statementMap[name] = std::move(variableAccessStatement);
}

bool RecordHandler::hasAttribute(const std::string& name) { return this->statementMap.count(name) == 1; }

ExpressionStatementPtr RecordHandler::getAttribute(const std::string& name) {
    if (!hasAttribute(name)) {
        NES_ASSERT2_FMT(hasAttribute(name), "RecordHandler: Attribute name: " << name << " is not registered.");
    }
    return this->statementMap[name];
}
}// namespace NES::QueryCompilation