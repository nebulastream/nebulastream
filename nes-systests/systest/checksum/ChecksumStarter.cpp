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

#include <array>
#include <exception>
#include <fstream>
#include <iostream>
#include <string>
#include <argparse/argparse.hpp>
#include <Checksum.hpp>

int main(int argc, char* argv[])
{
    argparse::ArgumentParser program(
        R"(Checksum Tool.
This Tool calculates a checksum from a CSV file.
The output will contain: 1. Number of Rows, 2. Checksum of all values.
The checksum uses addition thus is resilient (or blind) to reordered tuples (or fields).
)",
        "1.0");
    program.add_argument("FILE").help("Input file path").default_value(std::string(""));

    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err)
    {
        std::cerr << err.what() << '\n';
        std::cerr << program;
        return 1;
    }

    std::istream* inputStream = &std::cin;
    std::ifstream file;

    const auto filePath = program.get<std::string>("FILE");
    if (!filePath.empty())
    {
        file.open(filePath, std::ios::binary);
        if (!file)
        {
            std::cerr << "Error: Cannot open file." << '\n';
            return 1;
        }
        inputStream = &file;

        file.seekg(0, std::ios::end);
        if (file.tellg() == 0)
        {
            std::cerr << "Error: File is empty." << '\n';
            return 1;
        }
        file.seekg(0, std::ios::beg);
    }
    else
    {
        if (std::cin.peek() == std::istream::traits_type::eof())
        {
            std::cerr << "Error: No input provided." << '\n';
            return 1;
        }
    }

    Checksum checksum;
    while (!inputStream->eof())
    {
        std::array<char, 8192> buffer{};
        inputStream->read(buffer.data(), buffer.size());
        const std::streamsize bytesRead = inputStream->gcount();
        checksum.add(std::string_view(buffer.data(), bytesRead));
    }

    std::cout << checksum << '\n';
    return 0;
}
