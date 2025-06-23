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

#include <Nautilus/DataTypes/VarVal.hpp>

template <typename T>
static nautilus::val<T*> getMemberWithOffset(nautilus::val<int8_t*> objectReference, const size_t memberOffset)
{
    #pragma GCC diagnostic ignored "-Wnull-pointer-subtraction"
    return static_cast<nautilus::val<T*>>(objectReference + memberOffset); /// NOLINT
}
template <typename T>
static nautilus::val<T**> getMemberPtrWithOffset(nautilus::val<T*> objectReference, const size_t memberOffset)
{
    #pragma GCC diagnostic ignored "-Wnull-pointer-subtraction"
    return static_cast<nautilus::val<T**>>(objectReference + memberOffset); /// NOLINT
}

template <typename T>
nautilus::val<T*> dereferenceNautilusPtrPtr(nautilus::val<T**> nautilusPtrPtr)
{
    return nautilus::invoke(
            +[](T** nautilusPtrPtr) { return *nautilusPtrPtr; }, nautilusPtrPtr);
}
template <typename T>
nautilus::val<T*> getMemberPtrPLACEHOLDER(const nautilus::val<int8_t*>& objectReference, const size_t memberOffset)
{
    #pragma GCC diagnostic ignored "-Wnull-pointer-subtraction"
    nautilus::val<T**> memberPtrPtr = getMemberWithOffset<T*>(objectReference, memberOffset);
    return dereferenceNautilusPtrPtr(memberPtrPtr);
}