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

#ifndef NES_INCLUDE_VIEWS_MATERIALIZEDVIEW_HPP_
#define NES_INCLUDE_VIEWS_MATERIALIZEDVIEW_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <optional>

namespace NES::Experimental::MaterializedView {

// forward decl.
class MaterializedView;
using MaterializedViewPtr = std::shared_ptr<MaterializedView>;

/**
 * @brief this class provides the interface for all materialized views.
 * Materialized views in NES 'materialize' the result of streaming queries for further processing.
 */
class MaterializedView {

public:
    /// @brief constructor
    MaterializedView(uint64_t id) : id(id) {};

    /// @brief default deconstructor
    virtual ~MaterializedView() = default;

    /// @brief returns the materialized view id
    uint64_t getId() {return id;}

    /// @brief writes a data tuple to the view
    virtual bool writeData(Runtime::TupleBuffer buffer) = 0;

    /// @brief recieve a data tuple from the view
    virtual std::optional<Runtime::TupleBuffer> receiveData() = 0;

    // @brief clear the materialized view
    virtual void clear() = 0;

protected:
    uint64_t id;

}; // class MaterializedView
} // namespace NES::Experimental::MaterializedView
#endif  // NES_INCLUDE_VIEWS_MATERIALIZEDVIEW_HPP_
