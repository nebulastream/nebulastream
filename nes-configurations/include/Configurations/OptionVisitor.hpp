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
#include <Configurations/BaseOption.hpp>
#include <magic_enum.hpp>

namespace NES::Configurations
{

class BaseConfiguration;
template <typename T>
class TypedBaseOption;
class OptionVisitor
{
public:
    virtual ~OptionVisitor() = default;
    template <typename T>
    void visit(TypedBaseOption<T>& option)
    {
        visitLeaf(option);
        /// bool first because it also counts as a unsigned integral
        if constexpr (std::same_as<bool, T>)
        {
            bool v = option.getValue();
            visitBool(v);
            option.setValue(static_cast<T>(v));
        }
        else if constexpr (std::signed_integral<T>)
        {
            ssize_t v = option.getValue();
            visitSignedInteger(v);
            option.setValue(static_cast<T>(v));
        }
        else if constexpr (std::unsigned_integral<T>)
        {
            size_t v = option.getValue();
            visitUnsignedInteger(v);
            option.setValue(static_cast<T>(v));
        }
        else if constexpr (std::floating_point<T>)
        {
            double v = option.getValue();
            visitFloat(v);
            option.setValue(static_cast<T>(v));
        }
        else if constexpr (std::convertible_to<std::string, T> && std::convertible_to<T, std::string>)
        {
            std::string v = option.getValue();
            visitString(v);
            option.setValue(static_cast<T>(v));
        }
        else if constexpr (std::is_enum_v<T>)
        {
            static_assert(std::unsigned_integral<magic_enum::underlying_type_t<T>>, "This is only implemented for unsigned enums");
            size_t v = static_cast<magic_enum::underlying_type_t<T>>(option.getValue());
            visitEnum(magic_enum::enum_name(option.getValue()), v);
            option.setValue(magic_enum::enum_cast<T>(v).value());
        }
        else
        {
            static_assert(false, "Not handled");
        }
    }

    /// For handling SequenceOption
    template <typename T>
    void visit(std::vector<T>& options)
    {
        for (auto& option : options)
        {
            visit(option);
        }
    }

    /// When starting a compound type. All subsequent visits are part of the CompoundType.
    virtual void push(BaseOption& o) = 0;
    /// When finishing a compound type
    virtual void pop(BaseOption& o) = 0;

protected:
    /// Called for every leaf element. This is called before calling the typed visit functions
    virtual void visitLeaf(BaseOption& o) = 0;

    /// Typed visit functions:

    ///we lose the concrete enum type, but sometimes we want to present the human-readable enum value so the visitEnum
    ///provides both: string value and underlying value.
    virtual void visitEnum(std::string_view enumName, size_t& underlying) = 0;
    virtual void visitUnsignedInteger(size_t&) = 0;
    virtual void visitSignedInteger(ssize_t&) = 0;
    virtual void visitFloat(double&) = 0;
    virtual void visitBool(bool&) = 0;
    virtual void visitString(std::string&) = 0;
};
}
