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

#include <Nautilus/CodeGen/CPP/LabeledSegment.hpp>

#include <set>
#include <sstream>
#include <utility>

namespace NES::Nautilus::CodeGen::CPP {

LabeledSegment::LabeledSegment(std::string label)
    : label(std::move(label))
{

}

std::string LabeledSegment::toString() const {
    std::stringstream code;
    code << label << ":" << "\n";
    code << Segment::toString();
    return code.str();
}

const std::string& LabeledSegment::getLabel() const {
    return label;
}

} // namespace NES::Nautilus::CodeGen::CPP
