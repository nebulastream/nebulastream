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

// InvokeMacros.hpp
#pragma once
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/common/FunctionAttributes.hpp>
#include <InvokeConfiguration.hpp>

#define NAUTILUS_TAGGED_INVOKE(tag, func, ...) \
    [&]() \
    { \
        switch (NES::InvokeConfig::instance().resolveMode(tag)) \
        { \
            case NES::InvokeMode::FnAttr: \
                return nautilus::invoke(NES::InvokeConfig::instance().resolveFnAttr(tag), func, __VA_ARGS__); \
            case NES::InvokeMode::Default: \
            default: \
                return nautilus::invoke(func, __VA_ARGS__); \
        } \
    }()

// Todo: use below once we integrated inlining
// #define NAUTILUS_TAGGED_INVOKE(tag, func, ...) \
//     [&]() \
//     { \
//         switch (NES::InvokeConfig::instance().resolveMode(tag)) \
//         { \
//             case NES::InvokeMode::Inline: \
//                 return NAUTILUS_INLINE nautilus::invoke(func, __VA_ARGS__); \
//             case NES::InvokeMode::FnAttr: \
//                 return nautilus::invoke(NES::InvokeConfig::instance().resolveFnAttr(tag), func, __VA_ARGS__); \
//             case NES::InvokeMode::Default: \
//             default: \
//                 return nautilus::invoke(func, __VA_ARGS__); \
//         } \
//     }()