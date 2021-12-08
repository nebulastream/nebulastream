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
/*#include <Runtime/MaterializedViewManager.hpp>
#include <MaterializedView/MaterializedView.hpp>
#include <MaterializedView/TupleStorage.hpp>
#include <map>
#include <string>
#include <cstdint>
#include <cstring>
#include <sstream>

namespace NES::Experimental::MaterializedView {


void MaterializedViewManager::createMView(Schema schema, uint64_t memAreaSize){
    MaterializedViewPtr mView =
            std::make_shared<MaterializedView>(MaterializedView(nextViewId, memAreaSize, schema));
    mViewMap.insert(std::make_pair(nextViewId++, mView));
}

MaterializedViewPtr MaterializedViewManager::getMViewById(uint64_t mViewId) {
    auto it = mViewMap.find(mViewId);
    if (it != mViewMap.end()){
        return it->second;
    } else {
        // TODO: invalid operands to binary expression ('const char [70]' and 'int')
        //NES_DEBUG("MaterializedViewManager::getMViewById: no materialized view with id: " << mViewId);
        return nullptr;
    }
}

bool MaterializedViewManager::deleteMView(uint64_t queryId) {
    if (mViewMap.erase(queryId) == 1) {
        return true;
    }
    return false;
}
}// namespace NES::Experimental::MaterializedView
 */