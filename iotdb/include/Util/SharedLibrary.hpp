
#ifndef SHARED_LIBRARY_HPP
#define SHARED_LIBRARY_HPP

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"

#include <memory>
#include <string>

namespace NES {

class SharedLibrary;
typedef std::shared_ptr<SharedLibrary> SharedLibraryPtr;

class SharedLibrary {
  public:
    ~SharedLibrary();

    static SharedLibraryPtr load(const std::string& file_path);
    void* getSymbol(const std::string& mangeled_symbol_name) const;

    template<typename Function>
    Function getFunction(const std::string& mangeled_symbol_name) const {
        return reinterpret_cast<Function>(getSymbol(mangeled_symbol_name));
    }

  private:
    SharedLibrary(void* shared_lib);
    void* shared_lib_;
};

}// namespace NES

#pragma GCC diagnostic pop

#endif /* SHARED_LIBRARY_HPP */
