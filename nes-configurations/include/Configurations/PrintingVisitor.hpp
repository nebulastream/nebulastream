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
#include <stack>
#include <string>
#include <string_view>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ISequenceOption.hpp>
#include <Configurations/ReadingVisitor.hpp>
#include <sys/types.h>

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
    void push(const BaseOption& option) override
    {
        os << std::string(indentCount * TAB_WIDTH, ' ') << option.getName() << ": " << '\n';
        indentCount++;
    }
    void pop(const BaseOption&) override { indentCount--; }

    void push(const ISequenceOption&) override { os << "["; }
    void pop(const ISequenceOption&) override { os << "]"; }

protected:
    void visitLeaf(const BaseOption& option) override { os << std::string(indentCount * TAB_WIDTH, ' ') << option.getName() << ':'; }
    void visitEnum(std::string_view enumName, const size_t&) override { os << enumName ; }
    void visitUnsignedInteger(const size_t& value) override { os << value;  }
    void visitSignedInteger(const ssize_t& value) override { os << value; }
    void visitDouble(const double& value) override { os << value; }
    void visitBool(const bool& value) override { os << value; }
    void visitString(const std::string& value) override { os << value; }

private:
    std::ostream& os;
    size_t indentCount = 0;
    static constexpr u_int TAB_WIDTH = 4;
};

/// The idea of PrintingVisitor is, to print out the root configuration
/// Prints option in following format: "name: description (defaultValue, type of value)", currently only support for defaultValue
class HelpVisitor final : public ReadingVisitor
{
public:
    explicit HelpVisitor(std::ostream& os) : os(os) { }
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
    void push(const BaseOption& option) override
    {
        os << std::string(indentCount * TAB_WIDTH, ' ') << option.getName() << ": " << option.getDescription() << '\n';
        indentCount++;

        inSequence.push(false);
    }
    void pop(const BaseOption&) override
    {
        inSequence.pop();
        indentCount--;
    }

    void push(const ISequenceOption& option) override
    {
        os << std::string(indentCount * TAB_WIDTH, ' ') << option.getName() << ": " << option.getDescription() << " (Multiple)";
        inSequence.push(true);
    }

    void pop(const ISequenceOption&) override
    {
        os << '\n';
        inSequence.pop();
    }

protected:
    void visitLeaf(const BaseOption&) override { }
    void visitEnum(std::string_view enumName, const size_t&) override
    {
        if (inSequence.top())
        {
            return;
        }
        os << " (" << enumName << ", Enum)";
    }
    void visitUnsignedInteger(const size_t& value) override
    {
        if (inSequence.top())
        {
            return;
        }
        os << " (" << value << ", Unsigned Integer)";
    }
    void visitSignedInteger(const ssize_t& value) override
    {
        if (inSequence.top())
        {
            return;
        }
        os << " (" << value << ", Signed Integer)";
    }
    void visitDouble(const double& value) override
    {
        if (inSequence.top())
        {
            return;
        }
        os << " (" << value << ", Float)";
    }
    void visitBool(const bool& value) override
    {
        if (inSequence.top())
        {
            return;
        }
        os << " (" << (value ? "True" : "False") << ", Bool)";
    }
    void visitString(const std::string& value) override
    {
        if (inSequence.top())
        {
            return;
        }
        os << " (" << value << ", String)";
    }

private:
    std::ostream& os;
    size_t indentCount = 0;
    static constexpr u_int TAB_WIDTH = 4;

    std::stack<bool> inSequence;
};

}
