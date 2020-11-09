/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
