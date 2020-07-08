#ifndef NES_QUERYPROCESSORSERVICE_HPP
#define NES_QUERYPROCESSORSERVICE_HPP

#include <memory>

namespace NES {

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class QueryProcessorService {
  public:
    explicit QueryProcessorService(QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog);
    int operator()();

  private:
    bool stopQueryProcessor = false;
    QueryCatalogPtr queryCatalog;
    TypeInferencePhasePtr typeInferencePhase;
};
}// namespace NES
#endif//NES_QUERYPROCESSORSERVICE_HPP
