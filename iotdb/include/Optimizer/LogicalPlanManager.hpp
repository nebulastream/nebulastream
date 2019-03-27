
#ifndef INCLUDE_OPTIMIZER_LOGICALPLANMANAGER_HPP_
#define INCLUDE_OPTIMIZER_LOGICALPLANMANAGER_HPP_
#include <API/InputQuery.hpp>
#include <iostream>
#include <map>

namespace iotdb {
class LogicalPlanManager {
  public:
    LogicalPlanManager() : currentID(0){};

    void addQuery(InputQuery* q) { queries.insert(std::make_pair(currentID++, q)); }

    void removeQuery(size_t id)
    {
        std::map<size_t, InputQuery*>::iterator it;
        it = queries.find(id);
        queries.erase(it);
    }
    void showQueries()
    {
        std::map<size_t, InputQuery*>::iterator it;
        for (it = queries.begin(); it != queries.end(); ++it) {
            std::cout << it->first << " => " << it->second << '\n';
        }
    }

  private:
    size_t currentID;
    std::map<size_t, InputQuery*> queries;
};

} // namespace iotdb
#endif /* INCLUDE_OPTIMIZER_LOGICALPLANMANAGER_HPP_ */
