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

#include <map>

namespace NES::Experimental::MaterializedView {

// forward decl.
class MaterializedView;
using MaterializedViewPtr = std::shared_ptr<MaterializedView>;
class MaterializedViewManager;
using MaterializedViewManagerPtr = std::shared_ptr<MaterializedViewManager>;

/// @brief enum of supported view types
enum ViewType { TUPLE_VIEW };

/**
 * @brief the materialized view manager creates and manages materialized views.
 */
class MaterializedViewManager {

public:
    /// @brief default constructor
    MaterializedViewManager() = default;

    /// @brief deconstructor
    ~MaterializedViewManager() = default;

    /// @brief check if view with viewId exists
    bool containsView(uint64_t viewId);

    /// @brief retrieve the view by viewId
    /// @note will throw an runtime_error if view does not exist.
    /// @note please check existance with 'containView()'
    MaterializedViewPtr getView(uint64_t viewId);

    /// @brief create a new view by view type
    MaterializedViewPtr createView(ViewType type);

    /// @brief create a new view by view type and assing given id
    /// @note will throw an runtime_error if view does not exist.
    /// @note please check existance with 'containView()'
    MaterializedViewPtr createView(ViewType type, uint64_t viewId);

    /// @brief delete view from mananger
    bool deleteView(uint64_t viewId);

    /// @brief print managed views
    std::string to_string();

  private:
    std::map<uint64_t, MaterializedViewPtr> viewMap;
    uint64_t nextViewId = 0;

}; // class MaterializedViewManager
} // namespace NES::Experimental::MaterializedView
#endif//NES_INCLUDE_RUNTIME_MATERIALIZEDVIEWMANAGER_HPP_
