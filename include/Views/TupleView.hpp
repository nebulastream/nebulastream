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

#ifndef NES_MATERIALIZEDVIEW_TUPLEVIEW_HPP
#define NES_MATERIALIZEDVIEW_TUPLEVIEW_HPP

#include <queue>
#include <Views/MaterializedView.hpp>

namespace NES::Experimental::MaterializedView {

class TupleView;
using TupleViewPtr = std::shared_ptr<TupleView>;

/**
 * @brief this materialized view stores incoming tuples as buffers.
 * @note this experimental mv allows only one consumer
 */
class TupleView : public MaterializedView {

    /// friend for construction
    friend class MaterializedViewManager;

    /// @brief constructor
    TupleView(uint64_t id) : MaterializedView(id) {};

    /// @brief create shared ptr
    static TupleViewPtr createTupleView(uint64_t id);

public:
    /// @brief standard deconstructor
    ~TupleView() = default;

    /// @brief return front queue element
    std::optional<Runtime::TupleBuffer> receiveData();

    /// @brief insert tuple as last queue element
    bool writeData(Runtime::TupleBuffer buffer);

    /// @brief clear all stored buffer
    void clear();

private:
    mutable std::mutex mutex;
    std::vector<Runtime::TupleBuffer> vector;
    // current position to receive data from. It is currently shared between all consumers
    uint64_t consumePosition = 0;

}; // class TupleView
} // namespace NES::Experimental::MaterializedView
#endif //NES_MATERIALIZEDVIEW_TUPLEVIEW_HPP