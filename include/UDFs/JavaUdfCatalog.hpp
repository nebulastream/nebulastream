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

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "UDFs/JavaUdfImplementation.hpp"

namespace NES {

class JavaUdfCatalog {
  public:
    // TODO use unique_ptr here to make sure that JavaUdfCatalog takes ownership of the pointer
    void registerJavaUdf(const std::string& name, JavaUdfImplementationPtr implementation);
    JavaUdfImplementationPtr getUdfImplementation(const std::string& name);
    bool removeUdf(const std::string& name);

    // TODO Not sure if this is the correct API.
    // I'd rather return an iterator over the keys.
    // Or maybe we don't need this function at all.
    // It's not really used to support UDFs.
    // It could be used by a visualization tool.
    // It would be nice to show more information about the UDFs (signature, size of dependencies),
    // but that's not needed right now.
    const std::vector<std::string> listUdfs() const;

  private:
    std::unordered_map<std::string, JavaUdfImplementationPtr> udf_store_;
};

}