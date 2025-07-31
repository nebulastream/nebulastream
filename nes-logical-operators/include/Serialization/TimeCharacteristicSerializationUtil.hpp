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

#include "WindowTypes/Measures/TimeCharacteristic.hpp"


#include <SerializableVariantDescriptor.pb.h>
namespace NES
{
class TimeCharacteristicSerializationUtil
{
public:
    static SerializableTimeCharacteristic* serializeCharacteristic(const Windowing::TimeCharacteristic& characteristic, SerializableTimeCharacteristic* serializedCharacteristic);

    ///@return the unbound time characteristic, or nullopt if the argument contained a bound FieldAccessLogicalFunction
    static std::optional<Windowing::UnboundTimeCharacteristic> deserializeUnboundCharacteristic(const SerializableTimeCharacteristic& serializedCharacteristic);

    ///@return the bound time characteristic, or nullopt if the argument contained an UnboundFieldAccessLogicalFunction
    static std::optional<Windowing::BoundTimeCharacteristic> deserializeBoundCharacteristic(const SerializableTimeCharacteristic& serializedCharacteristic, const Schema& schema);
};

}