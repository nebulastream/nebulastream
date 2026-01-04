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

#include <Configurations/ConfigSerialization.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

Reflected Reflector<EnumWrapper>::operator()(const EnumWrapper&) const
{
    throw NotImplemented("Reflector");
}

EnumWrapper Unreflector<EnumWrapper>::operator()(const Reflected&) const
{
    throw NotImplemented("Reflector");
}

Reflected Reflector<FunctionList>::operator()(const FunctionList&) const
{
    throw NotImplemented("Reflector");
}

FunctionList Unreflector<FunctionList>::operator()(const Reflected&) const
{
    throw NotImplemented("Reflector");
}

Reflected Reflector<AggregationFunctionList>::operator()(const AggregationFunctionList&) const
{
    throw NotImplemented("Reflector");
}

AggregationFunctionList Unreflector<AggregationFunctionList>::operator()(const Reflected&) const
{
    throw NotImplemented("Reflector");
}

Reflected Reflector<WindowInfos>::operator()(const WindowInfos&) const
{
    throw NotImplemented("Reflector");
}

WindowInfos Unreflector<WindowInfos>::operator()(const Reflected&) const
{
    throw NotImplemented("Reflector");
}

Reflected Reflector<ProjectionList>::operator()(const ProjectionList&) const
{
    throw NotImplemented("Reflector");
}

ProjectionList Unreflector<ProjectionList>::operator()(const Reflected&) const
{
    throw NotImplemented("Reflector");
}

Reflected Reflector<UInt64List>::operator()(const UInt64List&) const
{
    throw NotImplemented("Reflector");
}

UInt64List Unreflector<UInt64List>::operator()(const Reflected&) const
{
    throw NotImplemented("Reflector");
}
}