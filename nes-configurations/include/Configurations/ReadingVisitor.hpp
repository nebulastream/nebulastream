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
#include <concepts>
#include <cstddef>
#include <string>
#include <string_view>
#include <Configurations/BaseOption.hpp>
#include <sys/types.h>
#include <magic_enum.hpp>

namespace NES::Configurations
{

class BaseConfiguration;
template <typename T>
class TypedBaseOption;


/// This class is the basis of all visitors. The idea of a visitor is, to visit config options and perform something using/on them.
/// e.g. PrintingVisitor, which goes through config options and prints each of them.
class ReadingVisitor
{
public:
    virtual ~ReadingVisitor() = default;
    /// visits the value of a TypedBaseOption
    template <typename T>
    void visit(const TypedBaseOption<T>& option)
    {
        visitLeaf(option);
        /// bool before 'unsigned integral', because bool would be treated as an unsigned integral otherwise
        if constexpr (std::same_as<bool, T>)
        {
            bool value = option.getValue();
            visitBool(value);
        }
        else if constexpr (std::signed_integral<T>)
        {
            ssize_t value = option.getValue();
            visitSignedInteger(value);
        }
        else if constexpr (std::unsigned_integral<T>)
        {
            size_t value = option.getValue();
            visitUnsignedInteger(value);
        }
        else if constexpr (std::floating_point<T>)
        {
            double value = option.getValue();
            visitDouble(value);
        }
        else if constexpr (std::convertible_to<std::string, T> && std::convertible_to<T, std::string>)
        {
            std::string value = option.getValue();
            visitString(value);
        }
        else if constexpr (std::is_enum_v<T>)
        {
            static_assert(std::unsigned_integral<magic_enum::underlying_type_t<T>>, "This is only implemented for unsigned enums");
            size_t value = static_cast<magic_enum::underlying_type_t<T>>(option.getValue());
            visitEnum(magic_enum::enum_name(option.getValue()), value);
        }
        else
        {
            static_assert(false, "Not handled");
        }
    }

    /// The general idea of push and pop is to support nested configs.
    /// For example, we have a nested config A. With push(), the visitor starts working on config A.
    /// pop() is called later when the visitor finishes working on config A. This is done recursively,
    /// because A can contain a nested subconfig B, and B can contain a nested subconfig C, and so on.
    ///
    /// For the given example, it would look like this:
    /// "push(A)
    ///     push(B)
    ///         push(C)
    ///         pop(C)
    ///     pop(B)
    /// pop(A)"
    ///
    /// Start working on a BaseOption. All subsequent visits are part of the BaseOption.
    virtual void push(const BaseOption& option) = 0;
    /// When finishing a BaseOption.
    virtual void pop(const BaseOption& option) = 0;

    virtual void push(const ISequenceOption& sequenceOption) = 0;
    virtual void pop(const ISequenceOption& sequenceOption) = 0;

protected:
    /// Called for every leaf element. This is called before calling the typed visit functions
    virtual void visitLeaf(const BaseOption& option) = 0;

    /// Typed visit functions:

    /// we lose the concrete enum type, but sometimes we want to present the human-readable enum value,
    /// so the visitEnum provides both: string value and underlying value.
    virtual void visitEnum(std::string_view enumName, const size_t& value) = 0;
    virtual void visitUnsignedInteger(const size_t&) = 0;
    virtual void visitSignedInteger(const ssize_t&) = 0;
    virtual void visitDouble(const double&) = 0;
    virtual void visitBool(const bool&) = 0;
    virtual void visitString(const std::string&) = 0;
};
}
