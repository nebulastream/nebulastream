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

#include <Execution/Operators/SliceStore/FileDescriptors/FileDescriptors.hpp>

namespace NES::Runtime::Execution
{

// TODO disable buffers for bufferSize zero

FileWriter::FileWriter(
    const std::string& filePath,
    const std::function<char*()>& allocate,
    const std::function<void(char*)>& deallocate,
    const size_t bufferSize)
    : file(filePath + ".dat", std::ios::out | std::ios::trunc | std::ios::binary)
    , keyFile(filePath + "_key.dat", std::ios::out | std::ios::trunc | std::ios::binary)
    , writeBuffer(allocate())
    , writeKeyBuffer(nullptr)
    , writeBufferPos(0)
    , writeKeyBufferPos(0)
    , bufferSize(bufferSize)
    , allocate(allocate)
    , deallocate(deallocate)
{
    if (!file.is_open())
    {
        throw std::ios_base::failure("Failed to open file for writing");
    }
    if (!keyFile.is_open())
    {
        throw std::ios_base::failure("Failed to open key file for writing");
    }
}

FileWriter::~FileWriter()
{
    flushBuffer();
    deallocate(writeBuffer);
    deallocate(writeKeyBuffer);
}

void FileWriter::write(const void* data, size_t size)
{
    write(data, size, writeBuffer, writeBufferPos, file);
}

void FileWriter::writeKey(const void* data, size_t size)
{
    if (!writeKeyBuffer)
    {
        writeKeyBuffer = allocate();
    }

    write(data, size, writeKeyBuffer, writeKeyBufferPos, keyFile);
}

void FileWriter::flushBuffer()
{
    writeToFile(writeBuffer, writeBufferPos, file);
    writeToFile(writeKeyBuffer, writeKeyBufferPos, keyFile);
}

void FileWriter::write(const void* data, size_t& dataSize, char* buffer, size_t& bufferPos, std::ofstream& fileStream) const
{
    if (bufferSize == 0)
    {
        return writeToFile(static_cast<const char*>(data), dataSize, fileStream);
    }

    auto dataPtr = static_cast<const char*>(data);
    while (dataSize > 0)
    {
        const auto copySize = std::min(dataSize, bufferSize - bufferPos);
        std::memcpy(buffer + bufferPos, dataPtr, copySize);
        bufferPos += copySize;
        dataPtr += copySize;
        dataSize -= copySize;

        if (bufferPos == bufferSize)
        {
            writeToFile(buffer, bufferPos, fileStream);
        }
    }
}

void FileWriter::writeToFile(const char* buffer, size_t& bufferPos, std::ofstream& fileStream)
{
    if (bufferPos > 0)
    {
        if (!fileStream.write(buffer, bufferPos))
        {
            throw std::ios_base::failure("Failed to write to file");
        }
        bufferPos = 0;
    }
}

FileReader::FileReader(
    const std::string& filePath,
    const std::function<char*()>& allocate,
    const std::function<void(char*)>& deallocate,
    const size_t bufferSize)
    : file(filePath + ".dat", std::ios::in | std::ios::binary)
    , keyFile(filePath + "_key.dat", std::ios::in | std::ios::binary)
    , filePath(filePath)
    , readBuffer(allocate())
    , readKeyBuffer(nullptr)
    , readBufferPos(0)
    , readKeyBufferPos(0)
    , readBufferEnd(0)
    , readKeyBufferEnd(0)
    , bufferSize(bufferSize)
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
    // TODO enable once JoinMultipleStreams.test passes with FileBackedTimeBasedSliceStore
    file.close();
    //std::filesystem::remove(filePath + ".dat");
    deallocate(readBuffer);

    keyFile.close();
    //std::filesystem::remove(filePath + "_key.dat");
    deallocate(readKeyBuffer);
}

size_t FileReader::read(void* dest, const size_t size)
{
    return read(dest, size, readBuffer, readBufferPos, readBufferEnd, file);
}

size_t FileReader::readKey(void* dest, const size_t size)
{
    if (!readKeyBuffer)
    {
        readKeyBuffer = allocate();
    }

    return read(dest, size, readKeyBuffer, readKeyBufferPos, readKeyBufferEnd, keyFile);
}

size_t
FileReader::read(void* dest, const size_t& dataSize, char* buffer, size_t& bufferPos, size_t& bufferEnd, std::ifstream& fileStream) const
{
    if (bufferSize == 0)
    {
        return readFromFile(static_cast<char*>(dest), dataSize, fileStream);
    }

    auto destPtr = static_cast<char*>(dest);
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

size_t FileReader::readFromFile(char* buffer, const size_t& dataSize, std::ifstream& fileStream)
{
    fileStream.read(buffer, dataSize);
    if (fileStream.fail() && !fileStream.eof())
    {
        throw std::ios_base::failure("Failed to read from file");
    }
    return fileStream.gcount();
}

}
