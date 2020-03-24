#ifndef NES_PARTITIONMANAGER_HPP
#define NES_PARTITIONMANAGER_HPP

#include <Network/NetworkCommon.hpp>

namespace NES {
namespace Network {
class PartitionManager {
public:
    void registerSubpartition();
    void unregisterSubpartition();
private:

};
}
}

#endif //NES_PARTITIONMANAGER_HPP
