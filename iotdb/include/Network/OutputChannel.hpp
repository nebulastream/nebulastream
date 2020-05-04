#ifndef NES_OUTPUTCHANNEL_HPP
#define NES_OUTPUTCHANNEL_HPP

#include <boost/core/noncopyable.hpp>
#include <memory>
#include <Network/NetworkMessage.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <iostream>

namespace NES {
namespace Network {

/**
 * NOT THREAD SAFE! DON'T SHARE AMONG THREADS!
 *
 */
class OutputChannel : public boost::noncopyable {
  public:
    explicit OutputChannel(
        std::shared_ptr<zmq::context_t> zmqContext,
        QueryId queryId,
        OperatorId operatorId,
        PartitionId partitionId,
        SubpartitionId subpartitionId,
        const std::string& address)
        : zmqSocket(*zmqContext, ZMQ_DEALER),
          queryId(queryId),
          operatorId(operatorId),
          partitionId(partitionId),
          subpartitionId(subpartitionId),
          isClosed(false) {
        init(address);
    }

    ~OutputChannel() {
        close();
    }

  private:
    void init(const std::string& socketAddr);

  public:
    void sendBuffer(TupleBuffer& inputBuffer);

    void onError(/** error info **/);

    /**
     * Close the outchannel and send EndOfStream message to consumer
     */
    void close();

  private:
    zmq::socket_t zmqSocket;
    const QueryId queryId;
    const OperatorId operatorId;
    const PartitionId partitionId;
    const SubpartitionId subpartitionId;

    bool isClosed;

};
}
}

#endif //NES_OUTPUTCHANNEL_HPP
