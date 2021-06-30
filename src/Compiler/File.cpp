#include <Compiler/File.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <Util/Logger.hpp>
namespace NES::Compiler {

File::File(std::string path) : path(path) {}

std::string File::getPath() { return path; }

std::shared_ptr<File> File::createFile(std::string path, std::string content) {
    //NES_DEBUG("Compiler: write source to file://" << currentPath << "/" << path);
    std::ofstream resultFile(path, std::ios::trunc | std::ios::out);
    resultFile << content;
    return std::make_shared<File>(File(path));
}

void File::print() {
    // read source file in
    std::ifstream file(path);
    file.clear();
    std::string sourceCode((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    NES_DEBUG("Compiler: code \n" << sourceCode);
}

}// namespace NES::Compiler