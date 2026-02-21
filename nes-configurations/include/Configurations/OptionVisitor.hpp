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

#include <string>
#include <string_view>

namespace NES
{
class BaseConfiguration;

struct OptionInfo
{
    std::string name;
    std::string description;
    std::string_view defaultValue;
    std::string_view currentValue;
    bool isExplicitlySet = false;
};

class OptionVisitor
{
public:
    virtual ~OptionVisitor() = default;

    OptionVisitor() { }

    virtual void visitOption(const OptionInfo& info) = 0;

    virtual void push() = 0;
    virtual void pop() = 0;

    /// Called when entering/exiting a named configuration section.
    /// Default implementation delegates to push()/pop() for backward compatibility.
    virtual void pushSection(std::string_view /*sectionName*/) { push(); }

    virtual void popSection() { pop(); }
};
}
