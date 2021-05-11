#ifndef NES_INCLUDE_NODEENGINE_TRANSACTIONAL_TRANSACTIONID_HPP_
#define NES_INCLUDE_NODEENGINE_TRANSACTIONAL_TRANSACTIONID_HPP_
#include <NodeEngine/NesThread.hpp>
namespace NES::NodeEngine::Transactional {

class TransactionId {
  public:
    TransactionId(uint64_t counter);
    TransactionId(uint64_t counter, uint64_t originId, uint64_t threadId);
    bool operator <(const TransactionId &other) const;
    bool operator >(const TransactionId &other) const;
    uint64_t counter;
    uint64_t originId;
    uint64_t threadId;

};

}// namespace NES::NodeEngine::Transactional

#endif//NES_INCLUDE_NODEENGINE_TRANSACTIONAL_TRANSACTIONID_HPP_
