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

#include <Nautilus/Backends/CPP/CPPExecutable.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cstdint>
#include <utility>

namespace NES::Nautilus::Backends::CPP {

CPPExecutable::CPPExecutable(std::shared_ptr<Compiler::DynamicObject> obj) : obj(obj) {}

void* CPPExecutable::getInvocableFunctionPtr(const std::string&) { return obj->getInvocableMember<void*>("execute"); }

bool CPPExecutable::hasInvocableFunctionPtr() { return true; }

std::unique_ptr<Executable::GenericInvocable> CPPExecutable::getGenericInvocable(const std::string&) { return nullptr; }

}// namespace NES::Nautilus::Backends::CPP