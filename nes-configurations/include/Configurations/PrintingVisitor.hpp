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
#include "OptionVisitor.hpp"
#include "BaseConfiguration.hpp"

namespace NES::Configurations
{
class PrintingVisitor : public OptionVisitor
{
public:

    void enterBase(BaseConfiguration& baseConfiguration) override
    {
        currentPath.push_back(baseConfiguration.toString());
    }

    void exitBase(BaseConfiguration&) override
    {
        currentPath.pop_back();
    }

    void visitConcrete(std::string_view optionDefault) override
    {
        stringBuilder << " (Default: " << optionDefault << ")";
    }

    std::string toString()
    {
        return stringBuilder.str();
    }

    std::stringstream stringBuilder;
    std::vector<std::string> currentPath;
};
}