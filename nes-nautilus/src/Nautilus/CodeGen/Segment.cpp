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

#include <Nautilus/CodeGen/Segment.hpp>

#include <set>
#include <sstream>

namespace NES::Nautilus::CodeGen {

std::string Segment::toString() const {
    std::stringstream code;
    for (auto& codeGen : codeGenerators) {
        code << codeGen->toString();
    }
    return code.str();
}

void Segment::addToProlog(const std::shared_ptr<CodeGenerator>& codeGen) {
    prolog.push_back(codeGen);
}

const std::vector<std::shared_ptr<CodeGenerator>>& Segment::getProlog() const {
    return prolog;
}

void Segment::mergeProloguesToTopLevel() {
    for (const auto& codeGen : codeGenerators) {
        auto segment = std::dynamic_pointer_cast<Segment>(codeGen);
        if (segment) {
            segment->mergeProloguesToTopLevel();
            for (const auto& stmt : segment->prolog) {
                addToProlog(stmt);
            }
        }
    }
}

void Segment::add(const std::shared_ptr<CodeGenerator>& codeGen) {
    codeGenerators.push_back(codeGen);
}

} // namespace NES::Nautilus::CodeGen
