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
#include <SliceStore/FileDescriptor/FileDescriptors.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

FileWriter::FileWriter(
    boost::asio::io_context& ioCtx,
    std::string filePath,
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
    , filePath(std::move(filePath))
    , allocate(allocate)
    , deallocate(deallocate)
{
}

FileWriter::~FileWriter()
{
    flushAndDeallocateBuffers();
    file.close();
    keyFile.close();
}

boost::asio::awaitable<void> FileWriter::write(const void* data, const size_t size)
{
    if (not file.is_open())
    {
        openFile(file, filePath + ".dat");
    }
    co_await write(data, size, file, writeBuffer, writeBufferPos);
}

boost::asio::awaitable<void> FileWriter::writeKey(const void* data, const size_t size)
{
    if (not keyFile.is_open())
    {
        openFile(keyFile, filePath + "_key.dat");
    }
    co_await write(data, size, keyFile, writeKeyBuffer, writeKeyBufferPos);
}

boost::asio::awaitable<uint32_t> FileWriter::writeVarSized(const void* data)
{
    const auto filePathStr = filePath + fmt::format("_{}.dat", varSizedCnt);
    boost::asio::posix::stream_descriptor varSizedFile(ioCtx);
    openFile(varSizedFile, filePathStr);

    auto varSizedDataSize = *static_cast<const uint32_t*>(data) + sizeof(uint32_t);
    co_await writeToFile(static_cast<const char*>(data), varSizedDataSize, varSizedFile);
    varSizedFile.close();

    co_return varSizedCnt++;
}

bool FileWriter::hasBuffer() const
{
    return writeBuffer != nullptr or writeKeyBuffer != nullptr;
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

    boost::asio::post(ioCtx, [this] { std::filesystem::remove(filePath + ".dat"); });
    boost::asio::post(ioCtx, [this] { std::filesystem::remove(filePath + "_key.dat"); });
    for (auto i = 0UL; i < varSizedCnt; ++i)
    {
        boost::asio::post(ioCtx, [this, i] { std::filesystem::remove(filePath + fmt::format("_{}.dat", i)); });
    }
}

boost::asio::awaitable<void> FileWriter::flush()
{
    if (bufferSize == 0)
    {
        co_return;
    }

    if (file.is_open() and writeBuffer != nullptr and writeBufferPos > 0)
    {
        co_await writeToFile(writeBuffer, writeBufferPos, file);
    }
    if (keyFile.is_open() and writeKeyBuffer != nullptr and writeKeyBufferPos > 0)
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
        bufferPos = 0;
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

void FileWriter::openFile(boost::asio::posix::stream_descriptor& stream, const std::string& filePath)
{
    const auto fd = open(filePath.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0)
    {
        if (errno == EMFILE)
        {
            throw std::runtime_error("Failed to open file or key file for writing: Too many open files (EMFILE)");
        }
        else
        {
            throw std::runtime_error(fmt::format("Failed to open file or key file for writing: {}", std::strerror(errno)));
        }
    }
    stream.assign(fd);
}

FileReader::FileReader(
    boost::asio::io_context& ioCtx,
    std::string filePath,
    char* readBuffer,
    char* readKeyBuffer,
    const size_t bufferSize,
    const bool withCleanup)
    : ioCtx(ioCtx)
    , file(ioCtx)
    , keyFile(ioCtx)
    , readBuffer(readBuffer)
    , readKeyBuffer(readKeyBuffer)
    , readBufferPos(0)
    , readKeyBufferPos(0)
    , readBufferEnd(0)
    , readKeyBufferEnd(0)
    , bufferSize(bufferSize)
    , withCleanup(withCleanup)
    , filePath(std::move(filePath))
{
}

FileReader::~FileReader()
{
    file.close();
    keyFile.close();

    if (withCleanup)
    {
        std::filesystem::remove(filePath + ".dat");
        std::filesystem::remove(filePath + "_key.dat");
    }
}

boost::asio::awaitable<size_t> FileReader::read(void* dest, const size_t size)
{
    if (not file.is_open())
    {
        openFile(file, filePath + ".dat");
    }
    co_return co_await read(dest, size, file, readBuffer, readBufferPos, readBufferEnd);
}

boost::asio::awaitable<size_t> FileReader::readKey(void* dest, const size_t size)
{
    if (not keyFile.is_open())
    {
        openFile(keyFile, filePath + "_key.dat");
    }
    co_return co_await read(dest, size, keyFile, readKeyBuffer, readKeyBufferPos, readKeyBufferEnd);
}

boost::asio::awaitable<Memory::TupleBuffer> FileReader::readVarSized(Memory::AbstractBufferProvider* bufferProvider, uint32_t idx) const
{
    const auto filePathStr = filePath + fmt::format("_{}.dat", idx);
    boost::asio::posix::stream_descriptor varSizedFile(ioCtx);
    openFile(varSizedFile, filePathStr);

    uint32_t varSizedDataSize;
    co_await readFromFile(reinterpret_cast<char*>(&varSizedDataSize), sizeof(uint32_t), varSizedFile);

    if (auto buffer = bufferProvider->getUnpooledBuffer(varSizedDataSize + sizeof(uint32_t)); buffer.has_value())
    {
        *buffer.value().getBuffer<uint32_t>() = varSizedDataSize;
        co_await readFromFile(buffer.value().getBuffer<char>() + sizeof(uint32_t), varSizedDataSize, varSizedFile);

        varSizedFile.close();
        if (withCleanup)
        {
            std::filesystem::remove(filePathStr);
        }

        co_return buffer.value();
    }
    else
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available!");
    }
}

boost::asio::awaitable<size_t> FileReader::read(
    void* dest, const size_t dataSize, boost::asio::posix::stream_descriptor& stream, char* buffer, size_t& bufferPos, size_t& bufferEnd)
    const
{
    auto* destPtr = static_cast<char*>(dest);
    if (bufferSize == 0)
    {
        co_return co_await readFromFile(destPtr, dataSize, stream);
    }

    auto totalRead = 0UL;
    while (totalRead < dataSize)
    {
        if (bufferPos == bufferEnd)
        {
            bufferEnd = co_await readFromFile(buffer, bufferSize, stream);
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

    co_return totalRead;
}

boost::asio::awaitable<size_t> FileReader::readFromFile(char* buffer, const size_t dataSize, boost::asio::posix::stream_descriptor& stream)
{
    boost::system::error_code ec;
    const auto buf = boost::asio::buffer(buffer, dataSize);
    auto bytesRead = co_await stream.async_read_some(buf, boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    if (ec and ec != boost::asio::error::eof)
    {
        throw boost::system::system_error(ec);
        //throw std::ios_base::failure("Failed to read from file");
    }
    co_return bytesRead;
}

void FileReader::openFile(boost::asio::posix::stream_descriptor& stream, const std::string& filePath)
{
    const auto fd = open(filePath.c_str(), O_RDONLY, 0644);
    if (fd < 0)
    {
        if (errno == EMFILE)
        {
            throw std::runtime_error("Failed to open file or key file for reading: Too many open files (EMFILE)");
        }
        else
        {
            throw std::runtime_error(fmt::format("Failed to open file or key file for reading: {}", std::strerror(errno)));
        }
    }
    stream.assign(fd);
}

}
