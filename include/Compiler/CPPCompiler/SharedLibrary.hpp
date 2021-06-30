#ifndef NES_INCLUDE_COMPILER_CPPCOMPILER_SHAREDLIBRARY_HPP_
#define NES_INCLUDE_COMPILER_CPPCOMPILER_SHAREDLIBRARY_HPP_
#include <memory>
namespace NES::Compiler {

class SharedLibrary;
using SharedLibraryPtr = std::shared_ptr<SharedLibrary>;

class SharedLibrary {
  public:
    ~SharedLibrary();

    static SharedLibraryPtr load(const std::string& filePath);
    [[nodiscard]] void* getSymbol(const std::string& mangeldSymbolName) const;

    template<typename Function>
    Function getFunction(const std::string& mangeldSymbolName) const {
        return reinterpret_cast<Function>(getSymbol(mangeldSymbolName));
    }

    explicit SharedLibrary(void* shareLib);

  private:
    void* shareLib;
};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_CPPCOMPILER_SHAREDLIBRARY_HPP_
