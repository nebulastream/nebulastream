#include <NodeEngine/WorkerContext.hpp>

namespace NES {

WorkerContext::WorkerContext(uint32_t workerId) : workerId(workerId) {}

uint32_t WorkerContext::getId() const {
    return workerId;
}

void WorkerContext::storeChannel(Network::OperatorId id, Network::OutputChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator " << id << " for context " << workerId);
    channels[id] = std::move(channel);
}

void WorkerContext::releaseChannel(Network::OperatorId id) {
    NES_TRACE("WorkerContext: releasing channel for operator " << id << " for context " << workerId);
    channels[id]->close();
    channels.erase(id);
}

Network::OutputChannel* WorkerContext::getChannel(Network::OperatorId ownerId) {
    NES_TRACE("WorkerContext: retrieving channel for operator " << ownerId << " for context " << workerId);
    return channels[ownerId].get();
}

}