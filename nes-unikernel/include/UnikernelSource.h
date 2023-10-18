//
// Created by ls on 09.09.23.
//

#ifndef NES_UNIKERNELSOURCE_H
#define NES_UNIKERNELSOURCE_H

#include "NetworkSource.h"
#include "Runtime/Execution/UnikernelPipelineExecutionContext.h"

extern NES::Runtime::BufferManagerPtr the_buffermanager;
extern NES::Network::NetworkManagerPtr the_networkmanager;
extern NES::Runtime::WorkerContextPtr the_workercontext;

namespace NES::Unikernel {
template<typename Config>
class UnikernelSource {

  public:
    explicit UnikernelSource(UnikernelPipelineExecutionContextBase* c)
        : source(NES::Network::NetworkSource(
            the_buffermanager,
            *the_workerContext,
            the_networkmanager,
            NES::Network::NesPartition(Config::QueryID,
                                       Config::UpstreamOperatorID,
                                       Config::UpstreamPartitionID,
                                       Config::UpstreamSubPartitionID),
            NES::Network::NodeLocation(Config::UpstreamNodeID, Config::UpstreamNodeHostname, Config::UpstreamNodePort),
            Config::LocalBuffers,
            200ms,
            100,
            c,
            "Upstream")) {}

  public:
    void setup() {}

    void start() {
        source.bind();
        source.start();
    }

    void stop() { source.stop(); }

  private:
    NES::Network::NetworkSource source;
};
}// namespace NES::Unikernel

#endif//NES_UNIKERNELSOURCE_H
