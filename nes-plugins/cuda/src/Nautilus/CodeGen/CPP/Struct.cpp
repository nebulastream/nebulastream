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

#include <Nautilus/CodeGen/CPP/Struct.hpp>

#include <fmt/core.h>
#include <sstream>

namespace NES::Nautilus::CodeGen::CPP {

Struct::Struct(std::string name)
    : name(std::move(name))
{

}

void Struct::addField(std::string type, std::string name) {
    fields.push_back(fmt::format("{} {}", type, name));
}

const std::string& Struct::getName() {
    return name;
}

std::string Struct::toString() const {
    std::string fieldSegment;
    for (const auto& field : fields) {
        fieldSegment += field + ";\n";
    }

    std::stringstream code;
    code << fmt::format("struct {} {{\n", name);
    code << "// Public fields\n";
    code << "public:\n";
    code << fieldSegment;
    code << "}\n";
    return code.str();
}

} // namespace NES::Nautilus::CodeGen::CPP
