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
#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>
#include <Configurations/OptionVisitor.hpp>

namespace NES::Configurations
{
class PrintingVisitor final : public OptionVisitor
{
public:
    explicit PrintingVisitor(std::ostream& ostream) : os(ostream) { }

    void push() override { indent += 1; }

    void pop() override { indent -= 1; }

    void visitConcrete(std::string name, std::string description, std::string_view defaultValue) override
    {
        os << std::string(indent * 4, ' ') << "- " << name << ": " << description;
        if (!defaultValue.empty())
        {
            os << " (Default: " << defaultValue << ")";
        }
        os << "\n";
    }

private:
    size_t indent = 0;
    std::ostream& os;
};
}
