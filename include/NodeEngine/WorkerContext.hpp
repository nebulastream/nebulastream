#ifndef NES_WORKERCONTEXT_HPP_
#define NES_WORKERCONTEXT_HPP_

#include <Network/NesPartition.hpp>
#include <Network/OutputChannel.hpp>
#include <NodeEngine/NesThread.hpp>
#include <cstdint>
#include <memory>
#include <unordered_map>

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

    /**
     * @brief get current worker context thread id. This is assigned by calling NesThread::getId()
     * @return current worker context thread id
     */
    uint32_t getId() const;

    /**
     * @brief This stores an output channel for an operator
     * @param id of the operator that we want to store the output channel
     * @param channel the output channel
     */
    void storeChannel(Network::OperatorId id, Network::OutputChannelPtr&& channel);

    /**
     * @brief removes a registered output channel
     * @param id of the operator that we want to store the output channel
     */
    void releaseChannel(Network::OperatorId id);

    /**
     * @brief retrieve a registered output channel
     * @param ownerId id of the operator that we want to store the output channel
     * @return an output channel
     */
    Network::OutputChannel* getChannel(Network::OperatorId ownerId);
};
}// namespace NES
#endif//NES_WORKERCONTEXT_HPP_
