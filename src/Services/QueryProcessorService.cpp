#include <Services/QueryProcessorService.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryProcessorService::QueryProcessorService(QueryCatalogPtr queryCatalog) : queryCatalog(queryCatalog) {
    NES_INFO("QueryProcessorService()");
}

int QueryProcessorService::operator()() {
    
    while (true){
        const std::vector<QueryPlanPtr> queryBatch = queryCatalog->getQueriesToSchedule();
        if(queryBatch.empty()){
            //process the queries using a-query-at-a-time model
        }
    }
}

}