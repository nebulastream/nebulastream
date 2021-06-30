#ifndef NES_INCLUDE_COMPILER_FILE_HPP_
#define NES_INCLUDE_COMPILER_FILE_HPP_
#include <string>
#include <memory>
namespace NES::Compiler {

class File {
  public:
    static std::shared_ptr<File> createFile(std::string path, std::string content);
    std::string getPath();
    void print();
  private:
    File(std::string path);
    std::string path;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_FILE_HPP_
