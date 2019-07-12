#ifndef API_ENVIRONMENT_H
#define API_ENVIRONMENT_H


#include <API/Config.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <API/InputQuery.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Window.hpp>
#include <iostream>
#include <string>

namespace iotdb {

/** \brief the central abstraction for the user to define queries */
    class Environment {
    public:

        static Environment create(const Config& config);
        void executeQuery(const InputQuery& inputQuery);

        // helper operators
        void printInputQueryPlan(const InputQuery& inputQuery);


    private:
        Config config;
        Environment(const Config &config);
        void printInputQueryPlan(const OperatorPtr& curr, int depth);
    };

/* this function **executes** the code provided by the user and returns an InputQuery Object */
    const InputQuery createQueryFromCodeString(const std::string&);
    typedef std::shared_ptr<InputQuery> InputQueryPtr;

} // namespace iotdb

#endif
