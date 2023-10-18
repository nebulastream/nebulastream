//
// Created by ls on 09.09.23.
//

#ifndef NES_UNIKERNELSINK_H
#define NES_UNIKERNELSINK_H

#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>

extern NES::Runtime::BufferManagerPtr the_buffermanager;
extern NES::Network::NetworkManagerPtr the_networkmanager;
extern NES::Runtime::WorkerContextPtr the_workerContext;

namespace {
using namespace std::chrono_literals;
template<typename Config>
class UnikernelSink {
  public:
    void setup() { sink.setup(); }

    void writeBuffer(NES::Runtime::TupleBuffer& tupleBuffer) { sink.writeData(tupleBuffer, *the_workerContext); }

    void stop() { sink.shutdown(); }

  private:
    NES::Network::NetworkSink sink = NES::Network::NetworkSink(
        1,
        Config::QueryID,
        Config::QuerySubplanID,
        NES::Network::NodeLocation(Config::DownstreamNodeID, Config::DownstreamNodeHostname, Config::DownstreamNodePort),
        NES::Network::NesPartition(Config::QueryID,
                                   Config::DownstreamOperatorID,
                                   Config::DownstreamPartitionID,
                                   Config::DownstreamSubPartitionID),
        the_buffermanager,
        *the_workerContext,
        the_networkmanager,
        Config::OutputSchemaSizeInBytes,
        1,
        200ms,
        100);
};
}// namespace

#endif//NES_UNIKERNELSINK_H
