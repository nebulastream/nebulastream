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
#include "Enums/EnumWrapper.hpp"
#include "Util/Serialization.hpp"

#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
class ProjectionList;
class WindowInfos;
class AggregationFunctionList;
class FunctionList;

template <>
struct Reflector<EnumWrapper>
{
    Reflected operator()(const EnumWrapper& wrapper) const;
};

template <>
struct Unreflector<EnumWrapper>
{
    EnumWrapper operator()(const Reflected& rfl) const;
};

template <>
struct Reflector<FunctionList>
{
    Reflected operator()(const FunctionList& list) const;
};

template <>
struct Unreflector<FunctionList>
{
    FunctionList operator()(const Reflected& rfl) const;
};

template <>
struct Reflector<AggregationFunctionList>
{
    Reflected operator()(const AggregationFunctionList& list) const;
};

template <>
struct Unreflector<AggregationFunctionList>
{
    AggregationFunctionList operator()(const Reflected& rfl) const;
};

template <>
struct Reflector<WindowInfos>
{
    Reflected operator()(const WindowInfos& infos) const;
};

template <>
struct Unreflector<WindowInfos>
{
    WindowInfos operator()(const Reflected& rfl) const;
};

template <>
struct Reflector<ProjectionList>
{
    Reflected operator()(const ProjectionList& list) const;
};

template <>
struct Unreflector<ProjectionList>
{
    ProjectionList operator()(const Reflected& rfl) const;
};

template <>
struct Reflector<UInt64List>
{
    Reflected operator()(const UInt64List& list) const;
};

template <>
struct Unreflector<UInt64List>
{
    UInt64List operator()(const Reflected& rfl) const;
};

}