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

#include <fstream>

enum FileLayout : uint8_t
{
    NO_SEPARATION,
    SEPARATE_FILES_KEYS,
    SEPARATE_FILES_PAYLOAD
};

class FileWriter
{
public:
    explicit FileWriter(const std::string& filePath);
    ~FileWriter();

    void write(const void* data, std::size_t size);

private:
    std::ofstream file_;
};

class FileReader
{
public:
    explicit FileReader(const std::string& filePath);
    ~FileReader();

    std::size_t read(void* dest, std::size_t size);

private:
    std::ifstream file_;
};
