#ifndef NES_STORAGEHANDLERACQUIRERESOURCESEXCEPTION_HPP
#define NES_STORAGEHANDLERACQUIRERESOURCESEXCEPTION_HPP
#include <Exceptions/RequestExecutionException.hpp>

namespace NES::Exceptions {
class StorageHandlerAcquireResourcesException : public RequestExecutionException {
  public:
    explicit StorageHandlerAcquireResourcesException(const std::string& message);

  private:
    std::string message;
};
}

#endif//NES_STORAGEHANDLERACQUIRERESOURCESEXCEPTION_HPP
