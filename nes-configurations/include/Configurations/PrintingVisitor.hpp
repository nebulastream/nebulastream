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
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/OptionVisitor.hpp>

namespace NES::Configurations
{
class PrintingVisitor : public OptionVisitor
{
public:
    PrintingVisitor(std::ostream& ostream) : os(ostream) { }

    void visitConcrete(std::string name, std::string description, std::string_view defaultValue) override
    {
        os << indent << "- " << name << ": " << description << " " << "(Default: " << defaultValue << ")" << "\n";
    }

    std::ostream& os;
};
}
