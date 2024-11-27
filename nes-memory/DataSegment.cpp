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
#include <cstddef>
#include <cstdint>
#include <optional>


#include <Runtime/DataSegment.hpp>

namespace NES::Memory::detail
{

static constexpr uint64_t offsetMask = (static_cast<uint64_t>(1) << static_cast<uint64_t>(48)) - 1;

OnDiskLocation::OnDiskLocation(uint8_t fileID, uint64_t offset) noexcept
{
    data = 1;
    data <<= 15;
    data |= fileID;
    data <<= 48;
    data |= offset;
}
OnDiskLocation::OnDiskLocation(uint8_t fileID, uint64_t offset, bool notPreAllocated) noexcept
{
    data = 2 + (notPreAllocated ? 1 : 0);
    data <<= 15;
    data |= fileID;
    data <<= 48;
    data |= offset;
}
OnDiskLocation::OnDiskLocation() noexcept
{
    data = 1;
    data <<= 63;
}

// OnDiskLocation::OnDiskLocation(const OnDiskLocation& other) noexcept
// {
// data = other.data;
// }
// OnDiskLocation& OnDiskLocation::operator=(const OnDiskLocation& other) noexcept
// {
//     data = other.data;
//     return *this;
// }
uint64_t OnDiskLocation::getOffset() const noexcept
{
    //constexpr uint64_t mask = (1 << 48) - 1;
    return data & offsetMask;
}
uint8_t OnDiskLocation::getFileID() const noexcept
{
    constexpr uint64_t mask = ((static_cast<uint64_t>(1) << static_cast<uint64_t>(8)) - 1) << static_cast<uint64_t>(48);
    return (data & mask) >> 48;
}

bool OnDiskLocation::isNotPreAllocated() const noexcept
{
    constexpr uint64_t mask = static_cast<uint64_t>(1) << 62;
    return (data & mask) > 0;
}

InMemoryLocation::InMemoryLocation(const uint8_t* ptr) noexcept
{
    constexpr auto mask = (static_cast<uint64_t>(1) << static_cast<uint64_t>(62)) - 1;
    data = reinterpret_cast<uintptr_t>(ptr) & mask;
}

InMemoryLocation::InMemoryLocation(const uint8_t* ptr, bool notPreAllocated) noexcept
{
    constexpr auto mask = (static_cast<uint64_t>(1) << static_cast<uint64_t>(62)) - 1;
    data = notPreAllocated ? 1 : 0;
    data <<= 62;
    data |= reinterpret_cast<uintptr_t>(ptr) & mask;
}
InMemoryLocation::InMemoryLocation() noexcept
{
    data = 0;
}
uint8_t* InMemoryLocation::getPtr() const noexcept
{
    constexpr uintptr_t mask = (static_cast<uint64_t>(1) << static_cast<uint64_t>(62)) - 1;
    return reinterpret_cast<uint8_t*>(data & mask);
}
bool InMemoryLocation::isNotPreAllocated() const noexcept
{
    constexpr uint64_t mask = static_cast<uint64_t>(1) << 62;
    return (data & mask) > 0;
}
bool DataLocation::isNotPreAllocated() const noexcept
{
    constexpr uint64_t mask = static_cast<uint64_t>(1) << 62;
    return (inMemory.data & mask) > 0;
}

DataLocation::DataLocation() noexcept : inMemory(InMemoryLocation{})
{
}

DataLocation::DataLocation(const InMemoryLocation inMemory) noexcept : inMemory(inMemory)
{
}
DataLocation::DataLocation(const OnDiskLocation onDisk) noexcept : spilled(onDisk)
{
}
std::optional<InMemoryLocation> DataLocation::getInMemoryLocation() const
{
    if (getTag() == 0)
    {
        return std::optional{inMemory};
    }
    return std::nullopt;
}
std::optional<OnDiskLocation> DataLocation::getOnDiskLocation() const
{
    if (getTag() == 1)
    {
        return std::optional{spilled};
    }
    return std::nullopt;
}
inline bool DataLocation::operator==(const InMemoryLocation& other) const noexcept
{
    return this->inMemory.data == other.data;
}
inline bool DataLocation::operator==(const OnDiskLocation& other) const noexcept
{
    return this->spilled.data == other.data;
}
inline bool DataLocation::operator==(const DataLocation& other) const noexcept
{
    return this->inMemory.data == other.inMemory.data;
}

inline uint8_t DataLocation::getTag() const
{
    return inMemory.data >> 63;
}

uint64_t DataLocation::getRaw() const
{
    return this->inMemory.data;
}

template <DataLocationConcept T>
T DataSegment<T>::getLocation() const
{
    return location;
}
template <DataLocationConcept T>
uint32_t DataSegment<T>::getSize() const
{
    return size;
}

template <DataLocationConcept T>
DataSegment<T>::DataSegment(T location, const size_t size) noexcept : location(location), size(size)
{
}
template <DataLocationConcept T>
DataSegment<T>::DataSegment() noexcept : location(T()), size(0)
{
}


// template <>
// DataSegment<InMemoryLocation>::DataSegment() noexcept : location(InMemoryLocation{}), size(0)
// {
// }

template <DataLocationConcept T>
template <DataLocationConcept O>
requires std::is_same_v<T, DataLocation> && (std::is_same_v<O, InMemoryLocation> || std::is_same_v<O, OnDiskLocation>)
DataSegment<T>::DataSegment(const DataSegment<O>& other) noexcept : location(other.location), size(other.size)
{
}

template <DataLocationConcept T>
template <DataLocationConcept O>
requires std::is_same_v<T, DataLocation> && (std::is_same_v<O, InMemoryLocation> || std::is_same_v<O, OnDiskLocation>)
DataSegment<T>& DataSegment<T>::operator=(const DataSegment<O>& other) noexcept
{
    location = other.location;
    size = other.size;
    return *this;
}

template <DataLocationConcept T>
template <DataLocationConcept O>
bool DataSegment<T>::operator==(const DataSegment<O>& other) noexcept
{
    return this->location == other.location && this->size == other.size;
}

template <DataLocationConcept T>
template <DataLocationConcept O>
bool DataSegment<T>::operator==(DataSegment<O>& other) noexcept
{
    return this->location == other.location && this->size == other.size;
}

template <DataLocationConcept T>
template <DataLocationConcept O>
requires std::is_same_v<T, DataLocation> && (std::is_same_v<O, InMemoryLocation> || std::is_same_v<O, OnDiskLocation>)
DataSegment<T>::operator const DataSegment<O>&()
{
    //requires reinterpret cast, otherwise this operator overload would recursively call itself
    return *reinterpret_cast<DataSegment<O>*>(this);
}
template <DataLocationConcept T>
template <DataLocationConcept O>
requires std::is_same_v<T, DataLocation> && (std::is_same_v<O, InMemoryLocation> || std::is_same_v<O, OnDiskLocation>)
DataSegment<T>::operator const DataSegment<O>()
{
    //requires reinterpret cast, otherwise this operator overload would recursively call itself
    return *reinterpret_cast<DataSegment<O>*>(this);
}
template <DataLocationConcept T>
template <DataLocationConcept O>
requires std::is_same_v<O, DataLocation> && (std::is_same_v<T, InMemoryLocation> || std::is_same_v<T, OnDiskLocation>)
DataSegment<T>::operator DataSegment<O>&() noexcept
{
    //requires reinterpret cast, otherwise this operator overload would recursively call itself
    return *reinterpret_cast<DataSegment<O>*>(this);
}
template <DataLocationConcept T>
template <DataLocationConcept O>
requires std::is_same_v<O, DataLocation> && (std::is_same_v<T, InMemoryLocation> || std::is_same_v<T, OnDiskLocation>)
DataSegment<T>::operator DataSegment<O>() noexcept
{
    //requires reinterpret cast, otherwise this operator overload would recursively call itself
    return *reinterpret_cast<DataSegment<O>*>(this);
}


template <>
template <>
std::optional<DataSegment<InMemoryLocation>> DataSegment<DataLocation>::get() const noexcept
{
    if (!isSpilled())
    {
        const auto* seg = reinterpret_cast<const DataSegment<InMemoryLocation>*>(this);
        return *seg;
    }
    return std::nullopt;
}
template <>
template <>
std::optional<DataSegment<OnDiskLocation>> DataSegment<DataLocation>::get() const noexcept
{
    if (isSpilled())
    {
        const auto* seg = reinterpret_cast<const DataSegment<OnDiskLocation>*>(this);
        return *seg;
    }
    return std::nullopt;
}


template <DataLocationConcept T>
bool DataSegment<T>::isSpilled() const
{
    auto data_location = reinterpret_cast<const DataLocation*>(&location);
    bool cond = data_location->getTag();
    return cond;
}

template <DataLocationConcept T>
bool DataSegment<T>::isNotPreAllocated() const
{
    return location.isNotPreAllocated();
}
template class DataSegment<InMemoryLocation>;
template class DataSegment<OnDiskLocation>;
template class DataSegment<DataLocation>;


template DataSegment<DataLocation>::DataSegment(const DataSegment<InMemoryLocation>&);

// Equality operator
template bool DataSegment<InMemoryLocation>::operator== <DataLocation>(const DataSegment<DataLocation>&) noexcept;
template bool DataSegment<OnDiskLocation>::operator== <DataLocation>(const DataSegment<DataLocation>&) noexcept;
template bool DataSegment<DataLocation>::operator== <DataLocation>(const DataSegment<DataLocation>&) noexcept;

template DataSegment<InMemoryLocation>::operator DataSegment<DataLocation>() noexcept;
template DataSegment<InMemoryLocation>::operator DataSegment<DataLocation>&() noexcept;

template DataSegment<OnDiskLocation>::operator DataSegment<DataLocation>() noexcept;
template DataSegment<OnDiskLocation>::operator DataSegment<DataLocation>&() noexcept;
}