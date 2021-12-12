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

#include <API/Schema.hpp>
#include <Views/TupleView.hpp>
#include <Views/MaterializedView.hpp>
#include <Runtime/MaterializedViewManager.hpp>

namespace NES::Experimental::MaterializedView {

MaterializedViewPtr MaterializedViewManager::getView(size_t viewId) {
    MaterializedViewPtr view = nullptr;
    auto it = viewMap.find(viewId);
    if (it != viewMap.end()){
        view = it->second;
    } else {
        NES_INFO("MaterializedViewManager::getView: no view found with id " << viewId);
        NES_INFO("MaterializedViewManager::getView: available views: " << to_string());
        view = nullptr;
    }
    return view;
}

MaterializedViewPtr MaterializedViewManager::createView(ViewType type){
    return createView(type, nextViewId++);
}

MaterializedViewPtr MaterializedViewManager::createView(ViewType type, size_t viewId){
    MaterializedViewPtr view = nullptr;
    if (viewMap.find(viewId) == viewMap.end()){
        // viewId is not present in map
        if (type == TUPLE_VIEW) {
            view = TupleView::createTupleView(viewId);
            viewMap.insert(std::make_pair(viewId, view));
        }
    }
    return view;
}

bool MaterializedViewManager::deleteView(size_t viewId){
    return viewMap.erase(viewId);
}

std::string MaterializedViewManager::to_string(){
    std::string out = "";
    for(const auto& elem : viewMap)
    {
        out = out + "id: " + std::to_string(elem.first) + "\t";
    }
    return out;
}

} // namespace NES::Experimental::MaterializedView