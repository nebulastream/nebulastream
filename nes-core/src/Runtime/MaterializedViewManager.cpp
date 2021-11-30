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
namespace NES::Experimental {
#include <MaterializedView/MaterializedView.hpp>
#include <map>

MaterializedViewPtr MaterializedViewManager::getMViewById(uint64_t mViewId) { return mViewMap.get(mViewId); }

uint64_t MaterializedViewManager::createMView() {
    MaterializedViewPtr mView;
    return mViewMap.insert(std::make_pair(nextQueryId++, mView));
}

bool deleteMView(uint64_t queryId) {
    if (mViewMap.erase(queryId) == 1) {
        return true;
    }
    return false;
}
}// namespace NES::Experimental