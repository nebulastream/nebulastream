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
#include <Runtime/DataSegment.hpp>

namespace NES::Memory
{
/**
 * @brief Interface for buffer recycling mechanism
 */
class BufferRecycler
{
public:
    //Templated virtual member function are not a thing yet, so we just spell out the variants manually :(


    /**
     * @brief Interface method for pooled buffer recycling
     * @param buffer the buffer to recycle
     */
    virtual bool recycleSegment(detail::DataSegment<detail::DataLocation>&& buffer)
    {
        //Better would be a visitor pattern in DataSegment like in std::variant
        if (auto inMemorySegment = buffer.get<detail::InMemoryLocation>())
        {
            recycleSegment(std::move(*inMemorySegment));
            return true;
        }
        else
        {
            auto onDiskSegment = buffer.get<detail::OnDiskLocation>();
            return recycleSegment(std::move(*onDiskSegment));
        }
    };

    /**
     * @brief Interface method for pooled buffer recycling
     * @param buffer the buffer to recycle
     */
    virtual void recycleSegment(detail::DataSegment<detail::InMemoryLocation>&& buffer) = 0;


    /**
     * @brief Interface method for pooled buffer recycling
     * @param buffer the buffer to recycle
     */
    virtual bool recycleSegment(detail::DataSegment<detail::OnDiskLocation>&& buffer) = 0;

};

}
