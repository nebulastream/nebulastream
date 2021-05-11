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

#ifndef NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKMANAGER_HPP_
#define NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKMANAGER_HPP_

#include <NodeEngine/Transactional/TransactionId.hpp>
namespace NES::NodeEngine::Transactional {

using WatermarkTs = uint64_t;
class WatermarkManager;
typedef std::shared_ptr<WatermarkManager> WatermarkManagerPtr;

class WatermarkManager {
  public:
    virtual void updateWatermark(TransactionId& transactionId, WatermarkTs watermarkTs) = 0;
    virtual void update(TransactionId& transactionId) = 0;
    virtual WatermarkTs getCurrentWatermark(TransactionId& transactionId) = 0;

};

}// namespace NES::NodeEngine::Transactional

#endif//NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKMANAGER_HPP_
