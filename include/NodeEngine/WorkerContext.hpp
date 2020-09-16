#ifndef NES_WORKERCONTEXT_HPP_
#define NES_WORKERCONTEXT_HPP_

#include <NodeEngine/NesThread.hpp>
#include <cstdint>
#include <unordered_map>
#include <Network/NesPartition.hpp>
#include <Network/OutputChannel.hpp>
#include <memory>

namespace NES {


/**
 * @brief A WorkerContext represents the current state of a worker thread
 * Note that it is not thread-safe per se but it is meant to be used in
 * a thread-safe manner by the ThreadPool.
 */
class WorkerContext {
  private:

    /// the id of this worker context (unique per thread).
    uint32_t workerId;

    std::unordered_map<Network::OperatorId, Network::OutputChannelPtr> channels;

  public:
    explicit WorkerContext(uint32_t workerId);

    uint32_t getId() const;

    void storeChannel(Network::OperatorId id, Network::OutputChannelPtr&& channel);

    void releaseChannel(Network::OperatorId id);

    Network::OutputChannel* getChannel(Network::OperatorId ownerId);
};
}// namespace NES
#endif//NES_WORKERCONTEXT_HPP_
