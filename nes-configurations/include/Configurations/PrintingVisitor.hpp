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
#include <Configurations/ReadingVisitor.hpp>

namespace NES::Configurations
{

/// The idea of PrintingVisitor is, to print out the root configuration
/// Prints option in following format: "name: description (defaultValue, type of value)", currently only support for defaultValue
class PrintingVisitor final : public ReadingVisitor
{
public:
    explicit PrintingVisitor(std::ostream& os) : os(os) { }
    /// Example:
    /// NestedConfig a which is also the root config, with sub-options: b, c, d
    /// NestedConfig c, with sub options: x
    ///
    /// For the given example, it would look like this:
    /// "push(a)
    ///     push(b)
    ///     pop(b)
    ///     push(c)
    ///         push(x)
    ///         pop(x)
    ///     pop(c)
    ///     push(d)
    ///     pop(d)
    /// pop(a)"
    ///
    /// The "(defaultValue, type)" part is also printed because of the option.accept(visitor) between each push and pop (see BaseConfiguration).
    /// This means that the option gives the visitor access to its values, so that the visitor can retrieve the values and print them,
    /// for example, with visitSignedInteger().
    ///
    /// Output through PrintingVisitor:
    /// a: nestedconfig
    ///     b: description (defaultValue, type)
    ///     c: nestedconfig
    ///         x: description (defaultValue, type)
    ///     d: description (defaultValue, type)
    ///
    void push(BaseOption& o) override
    {
        os << '\n' << std::string(indentCount * 4, ' ') << o.getName() << ": " << o.getDescription();
        indentCount++;
    }
    void pop(BaseOption&) override { indentCount--; }

protected:
    void visitLeaf(BaseOption&) override { }
    void visitEnum(std::string_view enumName, size_t&) override { os << " (" << enumName << ')'; }
    void visitUnsignedInteger(size_t& value) override { os << " (" << value << ", Unsigned Integer)"; }
    void visitSignedInteger(ssize_t& value) override { os << " (" << value << ", Signed Integer)"; }
    void visitFloat(double& value) override { os << " (" << value << ", Float)"; }
    void visitBool(bool& value) override { os << " (" << (value ? "True" : "False") << ", Bool)"; }
    void visitString(std::string& value) override { os << " (" << value << ", String)"; }

private:
    std::ostream& os;
    size_t indentCount = 0;
};
}
