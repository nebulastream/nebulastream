#ifndef NES_QUERYPROCESSORSERVICE_HPP
#define NES_QUERYPROCESSORSERVICE_HPP

#include <memory>

namespace NES {

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class QueryProcessorService {
  public:

    explicit QueryProcessorService(QueryCatalogPtr queryCatalog);
    int operator()();

  private:
    QueryCatalogPtr queryCatalog;
};
}
#endif//NES_QUERYPROCESSORSERVICE_HPP
