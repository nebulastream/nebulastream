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

bool MaterializedViewManager::containsView(uint64_t viewId) {
    return viewMap.contains(viewId);
}

MaterializedViewPtr MaterializedViewManager::getView(uint64_t viewId) {
    MaterializedViewPtr view = nullptr;
    auto it = viewMap.find(viewId);
    if (it != viewMap.end()){
        view = it->second;
    } else {
        NES_ERROR("MaterializedViewManager::getView: no view found with id " << viewId);
        throw std::runtime_error("Materialized view with given id does not exist.");
    }
    return view;
}

MaterializedViewPtr MaterializedViewManager::createView(ViewType type){
    while (!containsView(nextViewId++)) {};
    return createView(type, nextViewId);
}

MaterializedViewPtr MaterializedViewManager::createView(ViewType type, uint64_t viewId){
    MaterializedViewPtr view = nullptr;
    if (!viewMap.contains(viewId)){
        if (type == TUPLE_VIEW) {
            view = TupleView::createTupleView(viewId);
            viewMap.insert(std::make_pair(viewId, view));
        } else {
            throw std::invalid_argument("Unknown materialized view type");
        }
    } else {
        throw std::runtime_error("Materialized view with given id: " + std::to_string(viewId) + " does already exist.");
    }
    return view;
}

bool MaterializedViewManager::deleteView(uint64_t viewId){
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