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

#ifndef NES_LINEAGEMANAGER_H
#define NES_LINEAGEMANAGER_H

#include "AbstractLineageManager.hpp"
#include <Util/BufferSequenceNumber.hpp>
#include <mutex>
#include <unordered_map>
namespace NES::Runtime {

/**
 * @brief The Lineage Manager class stores map of all tuples that got their sequence number changed
 * by stateful operators
 */
class LineageManager : public AbstractLineageManager {
  public:
    /**
     * @brief Constructor, which creates a lineage manager
     */
    LineageManager() = default;

    /**
     * @brief Inserts a pair newId, oldId into lineage, where newId is a key
     * @param newId new sequence number that was created by a stateful operator
     * @param oldId old sequence number that the tuple had
     */
    void insertIntoLineage(BufferSequenceNumber newId, BufferSequenceNumber oldId) override;

    /**
     * @brief Deletes a pair<newId,oldId> from lineage manager
     * @param id newId of the tuple
     * @return true in case of a success trimming
     */
    bool trimLineage(BufferSequenceNumber id) override;

    /**
     * @brief Finds an old id for the tuple with a given id
     * @param id new id of the tuple
     * @return old id of the tuple
     */
    BufferSequenceNumber invertBuffer(BufferSequenceNumber id);

    /**
     * @brief Return current lineage size
     * @return Current lineage size
     */
    size_t getLineageSize() const override;

  private:
    std::unordered_map<BufferSequenceNumber, BufferSequenceNumber> lineage;
    mutable std::mutex mutex;
};

using LineageManagerPtr = std::shared_ptr<Runtime::LineageManager>;

}// namespace NES::Runtime

#endif//NES_LINEAGEMANAGER_H
