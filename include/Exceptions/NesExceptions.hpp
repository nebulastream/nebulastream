#ifndef NES_INCLUDE_EXCEPTIONS_NESEXCEPTIONS_HPP_
#define NES_INCLUDE_EXCEPTIONS_NESEXCEPTIONS_HPP_
#include <Network/NesPartition.hpp>
#include <stdexcept>
#include <Network/NetworkMessage.hpp>

/**
 * @brief This class should collect all nes specific exception
 */
namespace NES {
class ErrorWhileRevertingChanges : public std::exception {};

class NesNetworkError : public std::runtime_error {
  public:
    explicit NesNetworkError(NES::Network::Messages::ErrMessage& msg) : msg(msg), std::runtime_error("") {
    }

    const NES::Network::Messages::ErrMessage& getErrorMessage() const {
        return msg;
    }

  private:
    const NES::Network::Messages::ErrMessage msg;
};

}// namespace NES
#endif//NES_INCLUDE_EXCEPTIONS_NESEXCEPTIONS_HPP_
