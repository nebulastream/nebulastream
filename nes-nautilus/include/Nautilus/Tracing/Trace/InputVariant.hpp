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
#include <variant>
#include <Nautilus/Tracing/Trace/BlockRef.hpp>
#include <Nautilus/Tracing/Trace/ConstantValue.hpp>
#include <Nautilus/Tracing/Trace/FunctionCallTarget.hpp>
#include <Nautilus/Tracing/Trace/OpCode.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
namespace NES::Nautilus::Tracing
{
class None
{
};
using InputVariant = std::variant<ValueRef, ConstantValue, BlockRef, None, FunctionCallTarget>;
} /// namespace NES::Nautilus::Tracing
