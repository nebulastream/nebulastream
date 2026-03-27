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

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <istream>
#include <optional>
#include <string>
#include <vector>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <ErrorHandling.hpp>
#include "Util/Files.hpp"

namespace NES
{

template <std::endian Endian, typename T>
requires std::integral<T> || std::floating_point<T>
void write(std::ostream& os, T val)
{
    if constexpr (std::endian::native == Endian)
    {
        os.write(reinterpret_cast<const char*>(&val), sizeof(T));
        return;
    }

    if constexpr (std::integral<T>)
    {
        auto swapped = std::byteswap(val);
        os.write(reinterpret_cast<const char*>(&swapped), sizeof(T));
    }
    else if constexpr (sizeof(T) == 4)
    {
        auto swapped = std::bit_cast<uint32_t>(std::byteswap(std::bit_cast<uint32_t>(val)));
        os.write(reinterpret_cast<const char*>(&swapped), sizeof(T));
    }
    else if constexpr (sizeof(T) == 8)
    {
        auto swapped = std::bit_cast<uint64_t>(std::byteswap(std::bit_cast<uint64_t>(val)));
        os.write(reinterpret_cast<const char*>(&swapped), sizeof(T));
    }
}

template <std::endian Endian, typename T>
requires std::integral<T> || std::floating_point<T>
T read(std::istream& is)
{
    std::array<char, sizeof(T)> buf;
    is.read(buf.data(), sizeof(T));

    if constexpr (std::endian::native == Endian)
    {
        return std::bit_cast<T>(buf);
    }

    if constexpr (std::integral<T>)
    {
        return std::byteswap(std::bit_cast<T>(buf));
    }
    else if constexpr (sizeof(T) == 4)
    {
        return std::bit_cast<T>(std::byteswap(std::bit_cast<uint32_t>(buf)));
    }
    else if constexpr (sizeof(T) == 8)
    {
        return std::bit_cast<T>(std::byteswap(std::bit_cast<uint64_t>(buf)));
    }
}

void writeBuffersToDisk(const std::filesystem::path& filePath, const std::vector<NES::TupleBuffer>& buffers, size_t schemaSizeInBytes)
{
    std::ofstream file(filePath, std::ios::binary);
    if (not file)
    {
        throw std::runtime_error(fmt::format("Could not open file: {} while writing: {}", filePath, getErrorMessageFromERRNO()));
    }

    write<std::endian::little, uint64_t>(file, buffers.size());
    for (const auto& buffer : buffers)
    {
        write<std::endian::little, uint64_t>(file, buffer.getNumberOfTuples() * schemaSizeInBytes);
        write<std::endian::little, uint64_t>(file, buffer.getSequenceRange().start[0]);
        write<std::endian::little, uint64_t>(file, buffer.getSequenceRange().start[1]);
        write<std::endian::little, uint64_t>(file, buffer.getSequenceRange().start[2]);
        write<std::endian::little, uint64_t>(file, buffer.getSequenceRange().end[0]);
        write<std::endian::little, uint64_t>(file, buffer.getSequenceRange().end[1]);
        write<std::endian::little, uint64_t>(file, buffer.getSequenceRange().end[2]);
        write<std::endian::little, uint64_t>(file, buffer.getNumberOfTuples());
        write<std::endian::little, uint64_t>(file, buffer.getNumberOfChildBuffers());
        file.write(buffer.getAvailableMemoryArea<char>().data(), buffer.getNumberOfTuples() * schemaSizeInBytes);

        for (size_t i = 0; i < buffer.getNumberOfChildBuffers(); ++i)
        {
            auto child = buffer.loadChildBuffer(NES::VariableSizedAccess::Index(i));
            write<std::endian::little, uint64_t>(file, child.getAvailableMemoryArea().size());
            write<std::endian::little, uint64_t>(file, child.getNumberOfTuples());
            file.write(child.getAvailableMemoryArea<char>().data(), child.getAvailableMemoryArea().size());
        }
    }
}

std::vector<NES::TupleBuffer> readBuffersToDisk(const std::filesystem::path& filePath, NES::AbstractBufferProvider& bufferProvider)
{
    std::ifstream file(filePath, std::ios::binary);
    if (not file)
    {
        throw std::runtime_error(fmt::format("Could not open file: {} while reading: {}", filePath, getErrorMessageFromERRNO()));
    }

    const auto numberOfBuffers = read<std::endian::little, uint64_t>(file);
    std::vector<NES::TupleBuffer> result;
    result.reserve(numberOfBuffers);

    for (size_t i = 0; i < numberOfBuffers; ++i)
    {
        const auto bufferSize = read<std::endian::little, uint64_t>(file);
        INVARIANT(bufferProvider.getBufferSize() >= bufferSize, "Buffer size mismatch");

        const auto sequenceStart0 = read<std::endian::little, uint64_t>(file);
        const auto sequenceStart1 = read<std::endian::little, uint64_t>(file);
        const auto sequenceStart2 = read<std::endian::little, uint64_t>(file);
        const auto sequenceEnd0 = read<std::endian::little, uint64_t>(file);
        const auto sequenceEnd1 = read<std::endian::little, uint64_t>(file);
        const auto sequenceEnd2 = read<std::endian::little, uint64_t>(file);
        const auto numberOfTuples = read<std::endian::little, uint64_t>(file);
        const auto numberOfChildren = read<std::endian::little, uint64_t>(file);

        auto buffer = bufferProvider.getBufferBlocking();
        file.read(buffer.getAvailableMemoryArea<char>().data(), bufferSize);
        buffer.setSequenceRange(NES::SequenceRange(
            NES::FracturedNumber({sequenceStart0, sequenceStart1, sequenceStart2}),
            NES::FracturedNumber({sequenceEnd0, sequenceEnd1, sequenceEnd2})));
        buffer.setNumberOfTuples(numberOfTuples);

        for (size_t j = 0; j < numberOfChildren; ++j)
        {
            const auto childBufferSize = read<std::endian::little, uint64_t>(file);
            const auto numberOfTuples = read<std::endian::little, uint64_t>(file);
            auto childBuffer = bufferProvider.getUnpooledBuffer(childBufferSize).value();
            childBuffer.setNumberOfTuples(numberOfTuples);
            file.read(childBuffer.getAvailableMemoryArea<char>().data(), childBufferSize);
            std::ignore = buffer.storeChildBuffer(childBuffer);
        }
        result.emplace_back(std::move(buffer));
    }

    return result;
}

inline std::optional<std::filesystem::path>
findFileByName(const std::string& fileNameWithoutExtension, const std::filesystem::path& directory)
{
    if (!std::filesystem::exists(directory) || !std::filesystem::is_directory(directory))
    {
        return std::nullopt;
    }

    for (const auto& entry : std::filesystem::directory_iterator(directory))
    {
        if (entry.is_regular_file())
        {
            if (std::string stem = entry.path().stem().string(); stem == fileNameWithoutExtension)
            {
                return entry.path();
            }
        }
    }

    return std::nullopt;
}

}
