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

FileWriter::FileWriter(const std::string& filePath)
    : file(filePath, std::ios::out | std::ios::trunc | std::ios::binary)
    , keyFile(filePath + "_key", std::ios::out | std::ios::trunc | std::ios::binary)
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
    if (file.is_open())
    {
        file.close();
    }
    if (keyFile.is_open())
    {
        keyFile.close();
    }
}

void FileWriter::write(const void* data, const std::size_t size)
{
    if (!file.write(static_cast<const char*>(data), size))
    {
        throw std::ios_base::failure("Failed to write to file");
    }
}

void FileWriter::writeKey(const void* data, const std::size_t size)
{
    if (!keyFile.write(static_cast<const char*>(data), size))
    {
        throw std::ios_base::failure("Failed to write to key file");
    }
}

FileReader::FileReader(const std::string& filePath)
    : file(filePath, std::ios::in | std::ios::binary), keyFile(filePath + "_key", std::ios::in | std::ios::binary), filePath(filePath)
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
    if (file.is_open())
    {
        file.close();
    }
    if (keyFile.is_open())
    {
        keyFile.close();
    }
    // TODO enable once JoinMultipleStreams.test passes with FileBackedTimeBasedSliceStore
    //std::filesystem::remove(filePath);
    //std::filesystem::remove(filePath + "_key");
}

std::size_t FileReader::read(void* dest, const std::size_t size)
{
    file.read(static_cast<char*>(dest), size);
    if (file.fail() && !file.eof())
    {
        throw std::ios_base::failure("Failed to read from file");
    }
    return file.gcount();
}

std::size_t FileReader::readKey(void* dest, const std::size_t size)
{
    keyFile.read(static_cast<char*>(dest), size);
    if (keyFile.fail() && !keyFile.eof())
    {
        throw std::ios_base::failure("Failed to read from key file");
    }
    return keyFile.gcount();
}
