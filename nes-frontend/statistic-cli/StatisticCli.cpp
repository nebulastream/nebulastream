/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <chrono>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <argparse/argparse.hpp>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/status.h>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>
#include <Version.hpp>

namespace
{
std::optional<Metric> toMetric(const std::string& name)
{
    static const std::map<std::string, Metric> METRICS{
        {"AVG", METRIC_AVERAGE},
        {"AVERAGE", METRIC_AVERAGE},
        {"MIN", METRIC_MIN_VAL},
        {"MAX", METRIC_MAX_VAL},
        {"RATE", METRIC_RATE},
        {"SELECTIVITY", METRIC_SELECTIVITY},
        {"CARDINALITY", METRIC_CARDINALITY}};
    const auto found = METRICS.find(name);
    return found == METRICS.end() ? std::nullopt : std::optional<Metric>{found->second};
}

void fillDomain(CollectionDomain& domain, const argparse::ArgumentParser& arguments)
{
    auto* data = domain.mutable_data();
    data->set_logical_source_name(arguments.get<std::string>("--source"));
    data->set_field_name(arguments.get<std::string>("--field"));
}

void fillKey(StatisticKey& key, const argparse::ArgumentParser& arguments, const Metric metric)
{
    key.set_metric(metric);
    fillDomain(*key.mutable_domain(), arguments);
    auto* size = key.mutable_window_size();
    size->set_value(arguments.get<uint64_t>("--window-ms"));
    size->set_unit(TIME_UNIT_MILLISECONDS);
}

int report(const grpc::Status& status)
{
    std::cerr << "the statistic service answered " << status.error_code() << ": " << status.error_message() << "\n";
    return 1;
}
}

int main(const int argc, char** argv)
{
    if (NES::hasVersionFlag(argc, argv))
    {
        NES::printVersion("nes-statistic-cli");
        return 0;
    }

    argparse::ArgumentParser arguments("nes-statistic-cli");
    arguments.add_argument("command").help("one of collect, get, deregister");
    arguments.add_argument("--port").required().help("the statistic service port");
    arguments.add_argument("--source").default_value(std::string{}).help("the logical source name");
    arguments.add_argument("--field").default_value(std::string{}).help("the field to collect over");
    arguments.add_argument("--metric").default_value(std::string{"AVG"}).help("AVG, MIN, MAX, RATE, SELECTIVITY or CARDINALITY");
    arguments.add_argument("--window-ms").default_value(uint64_t{0}).scan<'u', uint64_t>().help("the window size in milliseconds");
    arguments.add_argument("--condition").default_value(std::string{"true"}).help("the SQL condition guarding the statistic");
    arguments.add_argument("--event-time-field").default_value(std::string{}).help("event time field; ingestion time when omitted");
    arguments.add_argument("--start").default_value(uint64_t{0}).scan<'u', uint64_t>().help("the probe window start");
    arguments.add_argument("--end").default_value(std::numeric_limits<uint64_t>::max()).scan<'u', uint64_t>().help("the probe window end");
    arguments.add_argument("--timeout-sec").default_value(uint64_t{120}).scan<'u', uint64_t>().help("the RPC deadline in seconds");

    try
    {
        arguments.parse_args(argc, argv);
    }
    catch (const std::exception& exception)
    {
        std::cerr << exception.what() << "\n" << arguments;
        return 2;
    }

    const auto command = arguments.get<std::string>("command");
    const auto metric = toMetric(arguments.get<std::string>("--metric"));
    if (not metric.has_value())
    {
        std::cerr << "unknown metric '" << arguments.get<std::string>("--metric") << "'\n" << arguments;
        return 2;
    }

    const auto endpoint = "127.0.0.1:" + arguments.get<std::string>("--port");
    const auto stub = StatisticControlService::NewStub(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));

    grpc::ClientContext context;
    context.set_wait_for_ready(true);
    context.set_deadline(
        std::chrono::system_clock::now() + std::chrono::seconds{static_cast<int64_t>(arguments.get<uint64_t>("--timeout-sec"))});
    std::cout << std::setprecision(15);

    if (command == "collect")
    {
        CollectStatisticRequest request;
        request.set_metric(metric.value());
        fillDomain(*request.mutable_domain(), arguments);
        auto* size = request.mutable_window_type()->mutable_tumbling()->mutable_size();
        size->set_value(arguments.get<uint64_t>("--window-ms"));
        size->set_unit(TIME_UNIT_MILLISECONDS);
        if (const auto eventTimeField = arguments.get<std::string>("--event-time-field"); eventTimeField.empty())
        {
            request.mutable_time_characteristic()->mutable_ingestion();
        }
        else
        {
            auto* event = request.mutable_time_characteristic()->mutable_event();
            event->set_field_name(eventTimeField);
            event->set_unit(TIME_UNIT_MILLISECONDS);
        }
        request.set_condition(arguments.get<std::string>("--condition"));

        CollectStatisticResponse response;
        if (const auto status = stub->CollectNewStatistic(&context, request, &response); not status.ok())
        {
            return report(status);
        }
        std::cout << response.query_id() << "," << response.statistic_id() << "," << (response.already_existed() ? "true" : "false")
                  << "\n";
        return 0;
    }

    if (command == "get")
    {
        GetStatisticsRequest request;
        fillKey(*request.add_keys(), arguments, metric.value());
        request.set_start_ts(arguments.get<uint64_t>("--start"));
        request.set_end_ts(arguments.get<uint64_t>("--end"));

        GetStatisticsResponse response;
        if (const auto status = stub->GetStatistics(&context, request, &response); not status.ok())
        {
            return report(status);
        }
        std::cout << (response.has_value() ? "true" : "false") << "," << response.value() << "\n";
        return 0;
    }

    if (command == "deregister")
    {
        DeregisterStatisticRequest request;
        fillKey(*request.mutable_key(), arguments, metric.value());

        DeregisterStatisticResponse response;
        if (const auto status = stub->DeregisterStatistic(&context, request, &response); not status.ok())
        {
            return report(status);
        }
        std::cout << (response.removed() ? "true" : "false") << "\n";
        return 0;
    }

    std::cerr << "unknown command '" << command << "'\n" << arguments;
    return 2;
}
