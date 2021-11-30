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

#ifndef NES_INCLUDE_RUNTIME_MATERIALIZEDVIEWMANAGER_HPP_
#define NES_INCLUDE_RUNTIME_MATERIALIZEDVIEWMANAGER_HPP_

#include <API/Schema.hpp>
#include <MaterializedView/MaterializedView.hpp>

namespace NES::Experimental {
/**
 * @brief ...
 */
class MaterializedViewManager : public std::enable_shared_from_this<MaterializedViewManager> {
    MaterializedViewManager() : nextQueryId(0){};

    void getMViewById(uint64_t queryId);

    void createMView(Schema schema, uint64_t memAreaSize = 64);

    void deleteMView(uint64_t queryId);

  private:
    std::map<uint64_t, MaterializedViewPtr> mViewMap;
    uint64_t nextQueryId;
};
using MaterializedViewManagerPtr = std::shared_ptr<MaterializedViewManager>;
}// namespace NES::Experimental
#endif//NES_INCLUDE_RUNTIME_MATERIALIZEDVIEWMANAGER_HPP_
