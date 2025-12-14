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

#include "Setsum.hpp"

#include <array>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

void printHelp(const char* progName)
{
    std::cout << "Usage: " << progName << " [FILE]\n"
              << "Calculates setsum from a file or stdin.\n"
              << "Options:\n"
              << "  -h, --help    Show this help message\n";
}

int main(int argc, char* argv[])
{
    std::string filePath;

    if (argc > 1)
    {
        std::string arg = argv[1];
        if (arg == "-h" || arg == "--help")
        {
            printHelp(argv[0]);
            return 0;
        }
        filePath = arg;
    }

    std::istream* inputStream = &std::cin;
    std::ifstream file;

    if (!filePath.empty())
    {
        file.open(filePath, std::ios::binary);
        if (!file)
        {
            std::cerr << "Error: Cannot open file: " << filePath << '\n';
            return 1;
        }
        inputStream = &file;
    }
    else if (std::cin.peek() == std::istream::traits_type::eof())
    {
        printHelp(argv[0]);
        return 1;
    }

    Setsum setsum;
    std::string line;
    while (std::getline(*inputStream, line))
    {
        // might be dangerous if the line is too long
        // however this is starter, so we don't expect long lines
        setsum.add(line);
    }

    std::cout << setsum << '\n';
    return 0;
}
