#ifndef NES_INCLUDE_NODEENGINE_TRANSACTIONAL_COMBINEDWATERMARKMANAGER_HPP_
#define NES_INCLUDE_NODEENGINE_TRANSACTIONAL_COMBINEDWATERMARKMANAGER_HPP_

#include <NodeEngine/Transactional/TransactionId.hpp>
#include <NodeEngine/Transactional/TransactionId.hpp>
namespace NES::NodeEngine::Transactional {

using WatermarkTs = uint64_t;
using OriginId = uint64_t;
class WatermarkManager;
typedef std::shared_ptr<WatermarkManager> WatermarkManagerPtr;

template<class WatermarkManager, int numberOfOrigins>
class CombinedWatermarkManager{
  public:
    CombinedWatermarkManager() : localWatermarkManagers() {
        for (auto i = 0; i < numberOfOrigins; i++) {
            localWatermarkManagers[i] = WatermarkManager::create();
        }
    }

    static std::shared_ptr<CombinedWatermarkManager> create(){
        return std::make_shared<CombinedWatermarkManager<WatermarkManager, numberOfOrigins>>();
    }

    void updateWatermark(TransactionId& transactionId, OriginId originId, WatermarkTs watermarkTs) {
        NES_ASSERT2_FMT(originId < numberOfOrigins, "OriginId: " << originId << " was not valid ");
        localWatermarkManagers[originId]->updateWatermark(transactionId, watermarkTs);
        for (auto watermarkManager : localWatermarkManagers) {
            watermarkManager->update(transactionId);
        }
    };

    WatermarkTs getCurrentWatermark(TransactionId& transactionId) {
        WatermarkTs maxWatermarkTs = UINT64_MAX;
        for (auto watermarkManager : localWatermarkManagers) {
            auto currentWatermark = watermarkManager->getCurrentWatermark(transactionId);
            maxWatermarkTs = std::min(maxWatermarkTs, currentWatermark);
        }
        return maxWatermarkTs;
    };

  private:
    std::array<WatermarkManagerPtr, numberOfOrigins> localWatermarkManagers;
};

}// namespace NES::NodeEngine::Transactional

#endif//NES_INCLUDE_NODEENGINE_TRANSACTIONAL_COMBINEDWATERMARKMANAGER_HPP_
