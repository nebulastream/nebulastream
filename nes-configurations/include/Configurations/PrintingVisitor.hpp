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
    explicit PrintingVisitor(std::ostream& os) : os(os) { }
    void push(BaseOption& o) override
    {
        os << '\n' << std::string(indent * 4, ' ') << o.getName() << ": " << o.getDescription();
        indent++;
    }
    void pop(BaseOption&) override { indent--; }

protected:
    void visitLeaf(BaseOption&) override { }
    void visitEnum(std::string_view enumName, size_t&) override { os << " (" << enumName << ')'; }
    void visitUnsignedInteger(size_t& v) override { os << " (" << v << ", Unsigned Integer)"; }
    void visitSignedInteger(ssize_t& v) override { os << " (" << v << ", Signed Integer)"; }
    void visitFloat(double& v) override { os << " (" << v << ", Float)"; }
    void visitBool(bool& v) override { os << " (" << (v ? "True" : "False") << ", Bool)"; }
    void visitString(std::string& v) override { os << " (" << v << ", String)"; }

private:
    std::ostream& os;
    size_t indent = 0;
};
}
