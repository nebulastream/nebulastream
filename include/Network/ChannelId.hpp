#ifndef NES_INCLUDE_NETWORK_CHANNELID_HPP_
#define NES_INCLUDE_NETWORK_CHANNELID_HPP_

#include <Network/NesPartition.hpp>
#include <NodeEngine/NesThread.hpp>

namespace NES {
namespace Network {

class ChannelId {
  public:
    explicit ChannelId(NesPartition nesPartition, uint32_t threadId) : nesPartition(nesPartition), threadId(threadId) {
        // nop
    }

    NesPartition getNesPartition() const {
        return nesPartition;
    }

    size_t getThreadId() const {
        return threadId;
    }

    std::string toString() const {
        return nesPartition.toString() + "(threadId=" + std::to_string(threadId) + ")";
    }

    friend std::ostream& operator<<(std::ostream& os, const ChannelId& channelId) {
        return os << channelId.toString();
    }

  private:
    const NesPartition nesPartition;
    const uint32_t threadId;
};
}// namespace Network
}// namespace NES
#endif//NES_INCLUDE_NETWORK_CHANNELID_HPP_
