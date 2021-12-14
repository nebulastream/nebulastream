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

#include <Runtime/TupleBuffer.hpp>
#include <Runtime/MaterializedViewManager.hpp>
#include <Views/MaterializedView.hpp>
#include <Views/TupleView.hpp>

namespace NES::Experimental::MaterializedView {

TupleViewPtr TupleView::createTupleView(uint64_t id){
    return std::shared_ptr<TupleView>(new TupleView(id));
};

std::optional<Runtime::TupleBuffer> TupleView::receiveData(){
    std::unique_lock<std::mutex> lock(mutex);
    if(!queue.empty()){
        NES_INFO("TupleView::receiveData: success. new queue size: " << queue.size());

        auto elem = queue.front();
        queue.pop();
        return elem;
    } else {
        NES_INFO("TupleView::receiveData: return nullopt. new queue size: " << queue.size());

        return std::nullopt;
    }
};

bool TupleView::writeData(Runtime::TupleBuffer buffer){
    std::unique_lock<std::mutex> lock(mutex);
    if (!buffer.isValid()) {
        NES_ERROR("TupleView::writeData: input buffer invalid");
        return false;
    }
    queue.push(buffer);
    NES_INFO("TupleView::writeData: success. new queue size: " << queue.size());
    return true;
};

void TupleView::clear(){
    std::unique_lock<std::mutex> lock(mutex);

    // clear queue
    std::queue<Runtime::TupleBuffer> empty;
    std::swap(queue, empty);
}

} // namespace NES::Experimental::MaterializedView