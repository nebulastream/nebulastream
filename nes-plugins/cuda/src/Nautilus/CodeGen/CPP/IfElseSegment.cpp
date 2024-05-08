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

#include <Nautilus/CodeGen/CPP/IfElseSegment.hpp>

#include <sstream>

namespace NES::Nautilus::CodeGen::CPP {

IfElseSegment::IfElseSegment(std::string condition, std::shared_ptr<Segment> ifSegment, std::shared_ptr<Segment> elseSegment)
    : condition(std::move(condition))
    , ifSegment(std::move(ifSegment))
    , elseSegment(std::move(elseSegment))
{
    add(this->ifSegment);
    add(this->elseSegment);
}

std::string IfElseSegment::toString() const {
    std::stringstream code;
    code << "if (" << condition << ") {\n";
    code << ifSegment->toString();
    code << "} else {\n";
    code << elseSegment->toString();
    code << "}\n";
    return code.str();
}

} // namespace NES::Nautilus::CodeGen::CPP
