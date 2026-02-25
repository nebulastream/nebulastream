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

namespace NES
{

/// Forward declaration of reflection types for use in headers.
/// This header provides the minimal declarations needed to declare Reflector and Unreflector specializations.
/// Include Util/Reflection.hpp in .cpp files to get the full implementation.

/// Reflected object - generic serialized representation
struct Reflected;

/// Context object for context-aware deserialization
class ReflectionContext;

/// Template for reflection (serialization) logic
template <typename T>
struct Reflector;

/// Template for unreflection (deserialization) logic
template <typename T>
struct Unreflector;

}
