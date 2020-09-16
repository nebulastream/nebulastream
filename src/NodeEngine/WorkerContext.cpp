#include <NodeEngine/WorkerContext.hpp>

namespace NES {

WorkerContext::WorkerContext(uint32_t workerId) : workerId(workerId) {}

uint32_t WorkerContext::getId() const {
    return workerId;
}

void WorkerContext::storeChannel(Network::OperatorId id, Network::OutputChannelPtr&& channel) {
    channels[id] = std::move(channel);
}

void WorkerContext::releaseChannel(Network::OperatorId id) {
    channels[id]->close();
    channels.erase(id);
}

Network::OutputChannel* WorkerContext::getChannel(Network::OperatorId ownerId) {
    return channels[ownerId].get();
}

}