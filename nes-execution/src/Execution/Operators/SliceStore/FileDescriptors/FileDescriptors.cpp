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

FileWriter::FileWriter(const std::string& filePath) : file_(filePath, std::ios::out | std::ios::trunc | std::ios::binary)
{
    if (!file_.is_open())
    {
        throw std::ios_base::failure("Failed to open file");
    }
}

FileWriter::~FileWriter()
{
    if (file_.is_open())
    {
        file_.close();
    }
}

void FileWriter::write(const void* data, const std::size_t size)
{
    if (!file_.write(static_cast<const char*>(data), size))
    {
        throw std::ios_base::failure("Failed to write to file");
    }
}

FileReader::FileReader(const std::string& filePath) : file_(filePath, std::ios::in | std::ios::binary)
{
    if (!file_.is_open())
    {
        throw std::ios_base::failure("Failed to open file");
    }
}

FileReader::~FileReader()
{
    if (file_.is_open())
    {
        file_.close();
    }
}

std::size_t FileReader::read(void* dest, const std::size_t size)
{
    file_.read(static_cast<char*>(dest), size);
    if (file_.fail() && !file_.eof())
    {
        throw std::ios_base::failure("Failed to read from file");
    }
    return file_.gcount();
}
