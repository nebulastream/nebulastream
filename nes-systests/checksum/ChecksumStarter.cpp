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

#include <fstream>
#include <iostream>
#include <string>
#include <argparse/argparse.hpp>


int main(int argc, char* argv[])
{
    argparse::ArgumentParser program("Checksum", "1.0");
    program.add_argument("-f", "--file").help("Input file path").default_value(std::string(""));

    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err)
    {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    std::istream* inputStream = &std::cin;
    std::ifstream file;
    bool fromFile = false;

    std::string filePath = program.get<std::string>("--file");
    if (!filePath.empty())
    {
        file.open(filePath, std::ios::binary);
        if (!file)
        {
            std::cerr << "Error: Cannot open file." << std::endl;
            return 1;
        }
        inputStream = &file;
        fromFile = true;

        file.seekg(0, std::ios::end);
        if (file.tellg() == 0)
        {
            std::cerr << "Error: File is empty." << std::endl;
            return 1;
        }
        file.seekg(0, std::ios::beg);
    }
    else
    {
        if (std::cin.peek() == std::istream::traits_type::eof())
        {
            std::cerr << "Error: No input provided." << std::endl;
            return 1;
        }
    }

    size_t numberOfLines = 0;
    size_t sumOfBytes = 0;
    char character;

    while (file.get(character))
    {
        sumOfBytes += static_cast<size_t>(character);
        if (character == '\n')
        {
            numberOfLines++;
        }
    }

    std::cout << numberOfLines << ", " << sumOfBytes << std::endl;

    file.close();
    return 0;
}
