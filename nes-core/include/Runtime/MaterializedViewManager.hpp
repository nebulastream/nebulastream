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
#include <Views/TupleStorage.hpp>
#include <Views/MaterializedView.hpp>
#include <map>

namespace NES::Experimental::MaterializedView {

/// @brief supported view types
enum ViewType { TUPLE_STORAGE };
// TODO: wrong use of friend specifier...
// We access friend classes private methods
extern TupleStoragePtr createTupleStorage(size_t id);

/**
 * @brief The materialized view manager creates and managed materialized views in a map.
 */
class MaterializedViewManager {
public:
    MaterializedViewManager() {
        NES_INFO("Create MaterializedViewManager");
    };
    // TODO: Which are nessesary?
    MaterializedViewManager(const MaterializedViewManager&) = delete;
    MaterializedViewManager& operator=(const MaterializedViewManager&) = delete;
    MaterializedViewManager(MaterializedViewManager&) = delete;
    MaterializedViewManager& operator=(MaterializedViewManager&) = delete;

    ~MaterializedViewManager() {
        NES_INFO("MaterializedViewManager::~MaterializedViewManager");
        std::map<size_t, MaterializedViewPtr> empty;
        std::swap(viewMap, empty);
    }

    MaterializedViewPtr getView(size_t viewId) {
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

    MaterializedViewPtr createView(ViewType type){
        return createView(type, nextViewId++);
    }

    MaterializedViewPtr createView(ViewType type, size_t viewId){
        MaterializedViewPtr view = nullptr;
        if (viewMap.find(viewId) == viewMap.end()){
            // viewId is not present in map
            if (type == TUPLE_STORAGE) {
                view = TupleStorage::createTupleStorage(viewId);
                viewMap.insert(std::make_pair(viewId, view));
            }
        }
        NES_INFO("MaterializedViewManager::getView: available views: " << to_string());
        return view;
    }

    bool deleteView(size_t viewId){
        if (viewMap.erase(viewId) == 1) {
            return true;
        }
        return false;
    }

    /// TODO overload operator
    std::string to_string(){
        std::string out = "";
        for(const auto& elem : viewMap)
        {
            // TODO: make pretty
            out = out + std::to_string(elem.first) + ", ";
        }
        return out;
    }
  private:
    /*TODO remove*/
    //MaterializedViewPtr view;

    std::map<size_t, MaterializedViewPtr> viewMap;
    size_t nextViewId = 0;
};
using MaterializedViewManagerPtr = std::shared_ptr<MaterializedViewManager>;
}// namespace NES::Experimental::MaterializedView
#endif//NES_INCLUDE_RUNTIME_MATERIALIZEDVIEWMANAGER_HPP_
