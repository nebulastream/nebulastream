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
#include <cstdint>
#include <optional>


namespace NES::Memory::detail
{

// class OnDiskLocation;
// class InMemoryLocation;
// union DataLocation;
// class MemorySegment;

// template <typename T>
// concept DataLocationConcept
//     = std::is_same_v<T, InMemoryLocation> || std::is_same_v<T, OnDiskLocation> || std::is_same_v<T, DataLocation>;
//
// template <DataLocationConcept T>
// class DataSegment;


class OnDiskLocation
{
    friend union DataLocation;
    //ext4 inodes limit is uint_32, so we should not use that many files
    //Because its used in a TaggedData wrapper the first 8 MSB might get overwritten
    //An uint_16 with 16KB block size permits 1GB files
    //An uint_32 with 16KB block size permits 64TB files, although ext4 only supports up to 16TB
    //If block size is flexible, than uint32 only permits 4 GB files, 48 bits permit 256TB
    //char offset[6];
public:
    ///For offset, everything above 48 bits is cut off
    explicit OnDiskLocation(uint8_t fileID, uint64_t offset) noexcept;
    OnDiskLocation(const OnDiskLocation&) noexcept = default;
    explicit OnDiskLocation() noexcept;

    OnDiskLocation& operator=(const OnDiskLocation&) = default;
    friend bool operator<(const OnDiskLocation& lhs, const OnDiskLocation& rhs) { return lhs.data < rhs.data; }
    friend bool operator<=(const OnDiskLocation& lhs, const OnDiskLocation& rhs) { return !(rhs < lhs); }
    friend bool operator>(const OnDiskLocation& lhs, const OnDiskLocation& rhs) { return rhs < lhs; }
    friend bool operator>=(const OnDiskLocation& lhs, const OnDiskLocation& rhs) { return !(lhs < rhs); }

    //max 48 bits
    uint64_t getOffset() const;
    ///Returns file id, valid file ids are at least one, 0 indicates an invalid location that was empty initialized
    uint8_t getFileID() const;

private:
    uint64_t data;
};

static_assert(std::is_trivially_copyable_v<OnDiskLocation>);

class InMemoryLocation
{
    friend union DataLocation;

private:
    //While pointer size can vary, we want to ensure that we have a consistent amount of space to also store something in the MSByte
    uint64_t data;

public:
    explicit InMemoryLocation(uint8_t* ptr) noexcept;
    explicit InMemoryLocation() noexcept;
    uint8_t* getPtr() noexcept;

    friend bool operator<(const InMemoryLocation& lhs, const InMemoryLocation& rhs) { return lhs.data < rhs.data; }
    friend bool operator<=(const InMemoryLocation& lhs, const InMemoryLocation& rhs) { return !(rhs < lhs); }
    friend bool operator>(const InMemoryLocation& lhs, const InMemoryLocation& rhs) { return rhs < lhs; }
    friend bool operator>=(const InMemoryLocation& lhs, const InMemoryLocation& rhs) { return !(lhs < rhs); }
};

static_assert(std::is_trivially_copyable_v<InMemoryLocation>);

union DataLocation
{
    friend class BufferControlBlock;
    InMemoryLocation inMemory;
    OnDiskLocation spilled;

public:
    explicit DataLocation() noexcept;
    DataLocation(InMemoryLocation inMemory) noexcept;
    DataLocation(OnDiskLocation onDisk) noexcept;
    inline std::optional<InMemoryLocation> getInMemoryLocation() const;
    inline std::optional<OnDiskLocation> getOnDiskLocation() const;

    inline bool operator==(const InMemoryLocation& other) const noexcept;
    inline bool operator==(const OnDiskLocation& other) const noexcept;
    inline bool operator==(const DataLocation& other) const noexcept;

    friend bool operator<(const DataLocation& lhs, const DataLocation& rhs) { return lhs.inMemory < rhs.inMemory; }
    friend bool operator<=(const DataLocation& lhs, const DataLocation& rhs) { return !(rhs < lhs); }
    friend bool operator>(const DataLocation& lhs, const DataLocation& rhs) { return rhs < lhs; }
    friend bool operator>=(const DataLocation& lhs, const DataLocation& rhs) { return !(lhs < rhs); }

    [[nodiscard]] inline uint8_t getTag() const;
    [[nodiscard]] inline uint64_t getRaw() const;
};

static_assert(sizeof(DataLocation) == 8);
static_assert(std::is_trivially_copyable_v<DataLocation>);

template <typename T>
concept DataLocationConcept = (std::is_same_v<T, InMemoryLocation> || std::is_same_v<T, OnDiskLocation> || std::is_same_v<T, DataLocation>) &&
    requires ()
{
    {T()} -> std::same_as<T>;
};

template <DataLocationConcept T>
class DataSegment
{
    template <DataLocationConcept O> friend class DataSegment;
    T location;
    size_t size;


    template <DataLocationConcept O>
    requires std::is_same_v<T, DataLocation> && (std::is_same_v<O, InMemoryLocation> || std::is_same_v<O, OnDiskLocation>)
    explicit operator const DataSegment<O>&();

    template <DataLocationConcept O>
    requires std::is_same_v<T, DataLocation> && (std::is_same_v<O, InMemoryLocation> || std::is_same_v<O, OnDiskLocation>)
    explicit operator const DataSegment<O>();

public:

    //Making all of these methods explicit does nothing to improve safety but just makes it super clunky to use
    explicit DataSegment(T location, size_t size) noexcept;
    DataSegment(const DataSegment&) noexcept = default;

    explicit DataSegment() noexcept;

    DataSegment& operator=(const DataSegment&) = default;

    template <DataLocationConcept O>
    requires std::is_same_v<T, DataLocation> && (std::is_same_v<O, InMemoryLocation> || std::is_same_v<O, OnDiskLocation>)
    DataSegment(const DataSegment<O>&) noexcept;

    template <DataLocationConcept O>
    requires std::is_same_v<T, DataLocation> && (std::is_same_v<O, InMemoryLocation> || std::is_same_v<O, OnDiskLocation>)
    DataSegment& operator=(const DataSegment<O>&) noexcept;

    template <DataLocationConcept O>
    bool operator==(const DataSegment<O>&) noexcept;

    template <DataLocationConcept O>
    bool operator==(DataSegment<O>&) noexcept;

    template <DataLocationConcept O>
    requires std::is_same_v<O, DataLocation> && (std::is_same_v<T, InMemoryLocation> || std::is_same_v<T, OnDiskLocation>)
    operator DataSegment<O>&() noexcept;

    template <DataLocationConcept O>
    requires std::is_same_v<O, DataLocation> && (std::is_same_v<T, InMemoryLocation> || std::is_same_v<T, OnDiskLocation>)
    operator DataSegment<O>() noexcept;

    template <DataLocationConcept O>
    requires std::is_same_v<T, DataLocation> && (std::is_same_v<O, InMemoryLocation> || std::is_same_v<O, OnDiskLocation>)
    [[nodiscard]] std::optional<DataSegment<O>> get() const noexcept;

    friend bool operator<(const DataSegment& lhs, const DataSegment& rhs) { return lhs.location < rhs.location; }
    friend bool operator<=(const DataSegment& lhs, const DataSegment& rhs) { return !(rhs < lhs); }
    friend bool operator>(const DataSegment& lhs, const DataSegment& rhs) { return rhs < lhs; }
    friend bool operator>=(const DataSegment& lhs, const DataSegment& rhs) { return !(lhs < rhs); }

    [[nodiscard]] T getLocation() const;
    [[nodiscard]] uint32_t getSize() const;
    [[nodiscard]] bool isSpilled() const;
};
static_assert(sizeof(DataSegment<DataLocation>) == 16);
static_assert(std::is_trivially_copyable_v<DataSegment<DataLocation>>);

}

template <NES::Memory::detail::DataLocationConcept T>
struct std::hash<NES::Memory::detail::DataSegment<T>>
{
    std::size_t operator()(const NES::Memory::detail::DataSegment<T>& segment) const
    {
        return segment.location;
    }
};
