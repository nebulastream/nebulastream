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

#include <DataGeneration/NEXMarkGeneration/GeneratorMediator.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

GeneratorMediator::GeneratorMediator(uint64_t size) {
    // create vectors of given size
}

GeneratorMediator* GeneratorMediator::getInstance(uint64_t size) {
    std::unique_lock<std::mutex> lock(mtxInstance);
    if (instance == nullptr) {
        instance = new GeneratorMediator(size);
    }
    lock.unlock();
    
    return instance;
}
} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
