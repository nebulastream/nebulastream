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

#include <string>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

Reflected Reflector<EnumWrapper>::operator()(const EnumWrapper& wrapper) const
{
    return reflect(wrapper.getValue());
}

EnumWrapper Unreflector<EnumWrapper>::operator()(const Reflected& rfl) const
{
    return EnumWrapper{unreflect<std::string>(rfl)};
}

Reflected Reflector<FunctionList>::operator()(const FunctionList&) const
{
    throw CannotSerialize("FunctionList is not expected to be reflected");
}

FunctionList Unreflector<FunctionList>::operator()(const Reflected&) const
{
    throw CannotDeserialize("FunctionList is not expected to be unreflected");
}

Reflected Reflector<AggregationFunctionList>::operator()(const AggregationFunctionList&) const
{
    throw CannotSerialize("AggregationFunctionList is not expected to be reflected");
}

AggregationFunctionList Unreflector<AggregationFunctionList>::operator()(const Reflected&) const
{
    throw CannotDeserialize("AggregationFunctionList is not expected to be unreflected");
}

Reflected Reflector<WindowInfos>::operator()(const WindowInfos&) const
{
    throw CannotSerialize("WindowInfos is not expected to be reflected");
}

WindowInfos Unreflector<WindowInfos>::operator()(const Reflected&) const
{
    throw CannotDeserialize("WindowInfos is not expected to be unreflected");
}

Reflected Reflector<ProjectionList>::operator()(const ProjectionList&) const
{
    throw CannotSerialize("ProjectionList is not expected to be reflected");
}

ProjectionList Unreflector<ProjectionList>::operator()(const Reflected&) const
{
    throw CannotDeserialize("ProjectionList is not expected to be unreflected");
}

Reflected Reflector<UInt64List>::operator()(const UInt64List&) const
{
    throw CannotSerialize("UInt64List is not expected to be reflected");
}

UInt64List Unreflector<UInt64List>::operator()(const Reflected&) const
{
    throw CannotDeserialize("UInt64List is not expected to be unreflected");
}
}
