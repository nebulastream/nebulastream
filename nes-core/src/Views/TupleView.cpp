/*
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
    // TODO: check performance of mutex since it could be a bottleneck. Replace with a workerContext
    std::unique_lock<std::mutex> lock(mutex);
    if(consumePosition < vector.size()){
        NES_INFO("TupleView::receiveData: success. current consumePosition: " << consumePosition);
        return vector[consumePosition++];
    } else {
        NES_INFO("TupleView::receiveData: failed. current consumePosition: " << consumePosition);
        return std::nullopt;
    }
};

bool TupleView::writeData(Runtime::TupleBuffer buffer){
    std::unique_lock<std::mutex> lock(mutex);
    if (!buffer.isValid()) {
        NES_ERROR("TupleView::writeData: input buffer invalid");
        return false;
    }
    vector.push_back(buffer);
    NES_INFO("TupleView::writeData: success. new vector size: " << vector.size());
    return true;
};

void TupleView::clear(){
    std::unique_lock<std::mutex> lock(mutex);

    // clear queue
    std::vector<Runtime::TupleBuffer> empty;
    std::swap(vector, empty);
    consumePosition = 0;
}

} // namespace NES::Experimental::MaterializedView