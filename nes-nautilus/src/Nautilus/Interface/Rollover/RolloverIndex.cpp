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
#include <Nautilus/Interface/Rollover/RolloverIndex.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Interface
{

template <typename T>
RolloverIndex<T>::RolloverIndex(const nautilus::val<T>& startIndex, const nautilus::val<T>& rolloverValue, Callback onRollover)
    : index(startIndex), overallIndex(startIndex), rolloverValue(rolloverValue), callback(std::move(onRollover))
{
}

template <typename T>
nautilus::val<T> RolloverIndex<T>::getCurrentIndex() const
{
    return index;
}

template <typename T>
nautilus::val<T> RolloverIndex<T>::getOverallIndex() const
{
    return overallIndex;
}

template <typename T>
void RolloverIndex<T>::increment()
{
    ++index;
    ++overallIndex;
    if (index >= rolloverValue)
    {
        index = 0;

        if (callback)
        {
            rolloverValue = callback(rolloverValue, overallIndex);
        }
    }
}

template class RolloverIndex<uint8_t>;
template class RolloverIndex<uint16_t>;
template class RolloverIndex<uint32_t>;
template class RolloverIndex<uint64_t>;
template class RolloverIndex<int8_t>;
template class RolloverIndex<int16_t>;
template class RolloverIndex<int32_t>;
template class RolloverIndex<int64_t>;
template class RolloverIndex<float>;
template class RolloverIndex<double>;

}
