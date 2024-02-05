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

#include <Nautilus/CodeGen/CPP/Function.hpp>

#include <fmt/core.h>

#include <set>
#include <sstream>

namespace NES::Nautilus::CodeGen::CPP {

Function::Function(std::vector<std::string> specifiers, std::string type, std::string name, std::vector<std::string> parameters)
    : specifiers(std::move(specifiers))
    , type(std::move(type))
    , name(std::move(name))
    , parameters(std::move(parameters))
{

}

std::string Function::toString() const {
    std::stringstream code;
    code << getSignature() + " {\n";
    code << "// Declarations\n";
    std::set<std::string> decls;
    for (const auto& segment : segments) {
        for (const auto& decl : segment->getProlog()) {
            decls.insert(decl->toString());
        }
    }
    for (const auto& decl : decls) {
        code << decl;
    }
    code << "// Basic blocks\n";
    for (const auto& segment : segments) {
        code << segment->toString();
    }
    code << "}\n";
    return code.str();
}

void Function::addSegment(const std::shared_ptr<Segment>& segment) {
    segments.push_back(segment);
}

std::string Function::getSignature() const {
    std::string specs;
    for (const auto& spec : specifiers) {
        specs += spec + " ";
    }

    std::string params;
    for (size_t i = 0; i < parameters.size(); i++) {
        if (i != 0) {
            params += ",";
        }
        params += parameters[i] + " ";
    }
    return fmt::format("{} {} {}({})", specs, type, name, params);
}

} // namespace NES::Nautilus::CodeGen::CPP
