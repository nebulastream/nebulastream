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

#include <filesystem>
#include <iostream>
#include <SliceStore/FileDescriptor/FileDescriptors.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

FileWriter::FileWriter(
    boost::asio::io_context& ioCtx,
    const std::string& filePath,
    const std::function<char*(const FileWriter* writer)>& allocate,
    const std::function<void(char*)>& deallocate,
    const size_t bufferSize)
    : ioCtx(ioCtx)
    , file(ioCtx)
    , keyFile(ioCtx)
    , writeBuffer(nullptr)
    , writeKeyBuffer(nullptr)
    , writeBufferPos(0)
    , writeKeyBufferPos(0)
    , bufferSize(bufferSize)
    , varSizedCnt(0)
    , filePath(filePath)
    , allocate(allocate)
    , deallocate(deallocate)
{
    const auto fd = open((filePath + ".dat").c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    const auto fdKey = open((filePath + "_key.dat").c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0 or fdKey < 0)
    {
        throw std::runtime_error("Failed to open file or key file for writing");
    }

    file.assign(fd);
    keyFile.assign(fdKey);
}

FileWriter::~FileWriter()
{
    flushAndDeallocateBuffers();
    file.close();
    keyFile.close();
}

boost::asio::awaitable<void> FileWriter::write(const void* data, const size_t size)
{
    co_await write(data, size, file, writeBuffer, writeBufferPos);
}

boost::asio::awaitable<void> FileWriter::writeKey(const void* data, const size_t size)
{
    co_await write(data, size, keyFile, writeKeyBuffer, writeKeyBufferPos);
}

boost::asio::awaitable<uint32_t> FileWriter::writeVarSized(const void* data)
{
    const auto filePathStr = filePath + fmt::format("_{}.dat", varSizedCnt);
    const auto fd = open(filePathStr.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0)
    {
        throw std::runtime_error("Failed to open variable sized data file for writing");
    }

    boost::asio::posix::stream_descriptor varSizedFile(ioCtx);
    varSizedFile.assign(fd);

    auto varSizedDataSize = *static_cast<const uint32_t*>(data) + sizeof(uint32_t);
    co_await writeToFile(static_cast<const char*>(data), varSizedDataSize, varSizedFile);
    varSizedFile.close();

    co_return varSizedCnt++;
}

void FileWriter::flushAndDeallocateBuffers()
{
    std::promise<void> promise;
    const std::future<void> future = promise.get_future();

    boost::asio::post(
        ioCtx,
        [this, &promise]
        {
            try
            {
                boost::asio::co_spawn(
                    ioCtx,
                    flush(),
                    [&promise](const std::exception_ptr& e)
                    {
                        if (e)
                        {
                            promise.set_exception(e);
                        }
                        else
                        {
                            promise.set_value();
                        }
                    });
            }
            catch ([[maybe_unused]] const std::exception& e)
            {
                promise.set_exception(std::current_exception());
            }
        });

    /// Use non-blocking calls to wait for the flush operation to complete
    while (future.wait_for(std::chrono::milliseconds(1)) != std::future_status::ready)
    {
        ioCtx.poll();
    }

    deallocateBuffers();
}

void FileWriter::deleteAllFiles()
{
    file.close();
    keyFile.close();

    std::filesystem::remove(filePath + ".dat");
    std::filesystem::remove(filePath + "_key.dat");
    for (auto i = 0UL; i < varSizedCnt; ++i)
    {
        std::filesystem::remove(filePath + fmt::format("_{}.dat", i));
    }
}

boost::asio::awaitable<void> FileWriter::flush()
{
    if (bufferSize == 0)
    {
        co_return;
    }

    if (writeBuffer != nullptr and writeBufferPos > 0)
    {
        co_await writeToFile(writeBuffer, writeBufferPos, file);
    }
    if (writeKeyBuffer != nullptr and writeKeyBufferPos > 0)
    {
        co_await writeToFile(writeKeyBuffer, writeKeyBufferPos, keyFile);
    }
}

void FileWriter::deallocateBuffers()
{
    deallocate(writeBuffer);
    writeBuffer = nullptr;
    deallocate(writeKeyBuffer);
    writeKeyBuffer = nullptr;
}

boost::asio::awaitable<void>
FileWriter::write(const void* data, size_t size, boost::asio::posix::stream_descriptor& stream, char*& buffer, size_t& bufferPos) const
{
    const auto* dataPtr = static_cast<const char*>(data);
    if (bufferSize == 0)
    {
        co_await writeToFile(dataPtr, size, stream);
        co_return;
    }
    if (buffer == nullptr)
    {
        buffer = allocate(this);
    }

    while (size > 0)
    {
        const auto copySize = std::min(size, bufferSize - bufferPos);
        std::memcpy(buffer + bufferPos, dataPtr, copySize);
        bufferPos += copySize;
        dataPtr += copySize;
        size -= copySize;

        if (bufferPos == bufferSize)
        {
            co_await writeToFile(buffer, bufferPos, stream);
        }
    }
}

boost::asio::awaitable<void> FileWriter::writeToFile(const char* buffer, size_t& size, boost::asio::posix::stream_descriptor& stream)
{
    const auto newBuffer = std::shared_ptr<char>(new char[size], std::default_delete<char[]>());
    std::memcpy(newBuffer.get(), buffer, size);

    co_await boost::asio::async_write(stream, boost::asio::buffer(newBuffer.get(), size), boost::asio::use_awaitable);
    size = 0;
}

FileReader::FileReader(
    const std::string& filePath,
    const std::function<char*()>& allocate,
    const std::function<void(char*)>& deallocate,
    const size_t bufferSize)
    : file(filePath + ".dat", std::ios::in | std::ios::binary)
    , keyFile(filePath + "_key.dat", std::ios::in | std::ios::binary)
    , readBuffer(nullptr)
    , readKeyBuffer(nullptr)
    , readBufferPos(0)
    , readKeyBufferPos(0)
    , readBufferEnd(0)
    , readKeyBufferEnd(0)
    , bufferSize(bufferSize)
    , filePath(filePath)
    , allocate(allocate)
    , deallocate(deallocate)
{
    if (!file.is_open())
    {
        throw std::ios_base::failure("Failed to open file for reading");
    }
    if (!keyFile.is_open())
    {
        throw std::ios_base::failure("Failed to open key file for reading");
    }
}

FileReader::~FileReader()
{
    file.close();
    deallocate(readBuffer);
    std::filesystem::remove(filePath + ".dat");

    keyFile.close();
    deallocate(readKeyBuffer);
    std::filesystem::remove(filePath + "_key.dat");
}

size_t FileReader::read(void* dest, const size_t size)
{
    return read(dest, size, file, readBuffer, readBufferPos, readBufferEnd);
}

size_t FileReader::readKey(void* dest, const size_t size)
{
    return read(dest, size, keyFile, readKeyBuffer, readKeyBufferPos, readKeyBufferEnd);
}

Memory::TupleBuffer FileReader::readVarSized(Memory::AbstractBufferProvider* bufferProvider, uint32_t idx) const
{
    const auto filePathStr = filePath + fmt::format("_{}.dat", idx);
    std::ifstream varSizedFile(filePathStr, std::ios::in | std::ios::binary);
    if (not varSizedFile.is_open())
    {
        throw std::runtime_error("Failed to open variable sized data file for reading");
    }

    uint32_t varSizedDataSize;
    varSizedFile.read(reinterpret_cast<char*>(&varSizedDataSize), sizeof(uint32_t));
    if ((varSizedFile.fail() and !varSizedFile.eof()) or varSizedFile.gcount() != sizeof(uint32_t))
    {
        throw std::ios_base::failure("Failed to read variable sized data size from file");
    }

    if (auto buffer = bufferProvider->getUnpooledBuffer(varSizedDataSize + sizeof(uint32_t)); buffer.has_value())
    {
        *buffer.value().getBuffer<uint32_t>() = varSizedDataSize;
        varSizedFile.read(buffer.value().getBuffer<char>() + sizeof(uint32_t), varSizedDataSize);
        if ((varSizedFile.fail() and !varSizedFile.eof()) or varSizedFile.gcount() != varSizedDataSize)
        {
            throw std::ios_base::failure("Failed to read variable sized data from file");
        }

        varSizedFile.close();
        std::filesystem::remove(filePathStr);

        return buffer.value();
    }
    else
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available!");
    }
}

size_t
FileReader::read(void* dest, const size_t dataSize, std::ifstream& fileStream, char*& buffer, size_t& bufferPos, size_t& bufferEnd) const
{
    auto* destPtr = static_cast<char*>(dest);
    if (bufferSize == 0)
    {
        return readFromFile(destPtr, dataSize, fileStream);
    }
    if (buffer == nullptr)
    {
        buffer = allocate();
    }

    auto totalRead = 0UL;
    while (totalRead < dataSize)
    {
        if (bufferPos == bufferEnd)
        {
            bufferEnd = readFromFile(buffer, bufferSize, fileStream);
            bufferPos = 0;
            if (bufferEnd == 0)
            {
                /// End of file
                break;
            }
        }

        const auto copySize = std::min(dataSize - totalRead, bufferEnd - bufferPos);
        std::memcpy(destPtr, buffer + bufferPos, copySize);
        bufferPos += copySize;
        destPtr += copySize;
        totalRead += copySize;
    }

    return totalRead;
}

size_t FileReader::readFromFile(char* buffer, const size_t dataSize, std::ifstream& fileStream)
{
    fileStream.read(buffer, dataSize);
    if (fileStream.fail() and !fileStream.eof())
    {
        throw std::ios_base::failure("Failed to read from file");
    }
    return fileStream.gcount();
}

}
