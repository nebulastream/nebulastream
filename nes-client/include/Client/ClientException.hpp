#ifndef NES_NES_CLIENT_INCLUDE_CLIENT_CLIENTEXCEPTION_HPP_
#define NES_NES_CLIENT_INCLUDE_CLIENT_CLIENTEXCEPTION_HPP_

#include <Exceptions/NesRuntimeException.hpp>

namespace NES {

class ClientException : public NesRuntimeException{
  public:
    explicit ClientException(const std::string& message);
};

}

#endif//NES_NES_CLIENT_INCLUDE_CLIENT_CLIENTEXCEPTION_HPP_
