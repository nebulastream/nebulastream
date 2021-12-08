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

#ifndef NES_MATERIALIZEDVIEWDELTASTORAGE_HPP
#define NES_MATERIALIZEDVIEWDELTASTORAGE_HPP

#include <Runtime/TupleBuffer.hpp>
#include <MaterializedView/MaterializedView.hpp>
#include <queue>

namespace NES::Experimental::MaterializedView {
/**
 * @brief This materialized view stores incomming tuples in a FIFO queue.
 */
class TupleStorage : public MaterializedView {
    /// Utilize the constructor
    friend class MaterializedViewManager;

    using TupleStoragePtr = std::shared_ptr<TupleStorage>;

    /// @brief standard constructor
    TupleStorage(size_t id) : MaterializedView(id) {};

    static TupleStoragePtr createTupleStorage(size_t id){
        return  std::shared_ptr<TupleStorage>(new TupleStorage(id));
        // TODO: why doesnt it work?
        //return std::make_shared<TupleStorage>(id);
    };

public:
    /// @brief return front queue element
    std::optional<Runtime::TupleBuffer> getData(){
        std::unique_lock<std::mutex> lock(mutex);
        if(!queue.empty()){
            auto elem = queue.back();
            queue.pop();
            return elem;
        } else {
            return std::nullopt;
        }
    };

    /// @brief insert tuple as last queue element
    void writeData(Runtime::TupleBuffer buffer){
        std::unique_lock<std::mutex> lock(mutex);
        if (!buffer.isValid()) {
            NES_ERROR("TupleStorage::writeData input buffer invalid");
            return;
        }
        NES_INFO("TupleStorage::WriteData: write tuple" << buffer.getBufferSize());
        queue.push(buffer);
    };

    /// @brief standard deconstructor
    ~TupleStorage() = default;

    void clear(){
        NES_INFO("TupleStorage::shutdown");
        std::unique_lock<std::mutex> lock(mutex);
        // clear queue
        std::queue<Runtime::TupleBuffer> empty;
        std::swap(queue, empty);
    }
private:
    mutable std::mutex mutex;
    std::queue<Runtime::TupleBuffer> queue;
};
using TupleStoragePtr = std::shared_ptr<TupleStorage>;
} // namespace NES::Experimental::MaterializedView
#endif //NES_MATERIALIZEDVIEWTUPLESTORAGE_HPP
