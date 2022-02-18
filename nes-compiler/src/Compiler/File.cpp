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
#include <Compiler/Util/File.hpp>
#include <Util/Logger.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <utility>
namespace NES::Compiler {

File::File(std::string path) : path(std::move(path)) {}

std::string File::getPath() const { return path; }

std::shared_ptr<File> File::createFile(const std::string& absoluteFilePath, const std::string& content) {
    NES_DEBUG("Create File to file://" << absoluteFilePath);
    std::ofstream resultFile(absoluteFilePath, std::ios::trunc | std::ios::out);
    resultFile << content;
    return std::make_shared<File>(absoluteFilePath);
}

std::string File::read() const {
    const std::lock_guard<std::mutex> fileLock(fileMutex);
    // read source file in
    std::ifstream file(path);
    file.clear();
    std::string sourceCode((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    return sourceCode;
}

void File::print() const {
    auto sourceCode = read();
    NES_DEBUG("Compiler: code \n" << sourceCode);
}

std::mutex& File::getFileMutex() { return fileMutex; }

}// namespace NES::Compiler