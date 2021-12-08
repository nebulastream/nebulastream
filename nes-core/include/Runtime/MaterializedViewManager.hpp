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
#include <MaterializedView/TupleStorage.hpp>
#include <MaterializedView/MaterializedView.hpp>
#include <map>

namespace NES::Experimental::MaterializedView {

/// @brief supported view types
enum ViewType { TUPLE_STORAGE };

// We access friend classes private methods
extern TupleStoragePtr createTupleStorage(size_t id);

/**
 * @brief The materialized view manager creates and managed materialized views in a hash map.
 */

class MaterializedViewManager : public std::enable_shared_from_this<MaterializedViewManager> {
public:
    MaterializedViewManager() = default;
    // TODO: WTF Rule of 5?
    MaterializedViewManager(const MaterializedViewManager&) = default;
    MaterializedViewManager(MaterializedViewManager&&) = default;
    MaterializedViewManager& operator=(const MaterializedViewManager&) = default;
    MaterializedViewManager& operator=(MaterializedViewManager&&) = default;

    ~MaterializedViewManager() {
        NES_INFO("MaterializedViewManager::~MaterializedViewManager");
        std::map<size_t, MaterializedViewPtr> empty;
        std::swap(viewMap, empty);
    }

    MaterializedViewPtr getView(size_t viewId) {
        auto it = viewMap.find(viewId);
        if (it != viewMap.end()){
            return it->second;
        } else {
            return nullptr;
        }
    }

    MaterializedViewPtr createView(ViewType type){
        MaterializedViewPtr view = nullptr;
        if (type == TUPLE_STORAGE) {
            view = TupleStorage::createTupleStorage(nextViewId);
            viewMap.insert(std::make_pair(nextViewId++, view));
        }
        return view;
    }

    bool deleteMView(size_t viewId){
        if (viewMap.erase(viewId) == 1) {
            return true;
        }
        return false;
    }

  private:
    std::map<size_t, MaterializedViewPtr> viewMap;
    size_t nextViewId = 0;
};
using MaterializedViewManagerPtr = std::shared_ptr<MaterializedViewManager>;
}// namespace NES::Experimental::MaterializedView
#endif//NES_INCLUDE_RUNTIME_MATERIALIZEDVIEWMANAGER_HPP_
