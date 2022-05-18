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

#ifndef NES_DUMMYMV_HPP
#define NES_DUMMYMV_HPP

/**
 * Dummy materialized view e.g. to measure the maximal possible throughput
 * @tparam K
 * @tparam V
 * @tparam I
 */
template <typename K = uint64_t, typename V = AMRecord, typename I = EventRecord>
class DummyMV: public MaterializedView<K,I> {
public:
    DummyMV() = default;

    /**
     * Inserts a new record by retrieving the current value and doing an update
     * @param key
     * @param event
     */
    void insert(K key, I event) {
       counter++;
    };

    /**
     * Clear the window (tumbling window)
     */
    void newWindow(){
       counter = 0;
    }

    /**
     * MV size
     * @return size
     */
    uint64_t size(){ return counter; }

    /**
     * MV name
     * @return name
     */
    std::string name() { return "Dummy Materialized View"; }

private:
    //std::atomic<uint64_t> counter = 0;
    uint64_t counter = 0;
};

#endif //NES_DUMMYMV_HPP
