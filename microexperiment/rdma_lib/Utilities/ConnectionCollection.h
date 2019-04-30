//
// Created by Toso, Lorenzo on 2019-01-11.
//

#pragma once

#include "VerbsConnection.h"
#include <vector>

class ConnectionCollection {
private:
    std::vector<std::shared_ptr<VerbsConnection>> connections;
public:
    ConnectionCollection() = default;

    explicit ConnectionCollection(const ConnectionInfoProvider & infoProvider);
    //ConnectionCollection(const std::vector<const ConnectionInfoProvider &> & infoProviders);

    void add_connection_to(const ConnectionInfoProvider & infoProvider);
    //void add_connections_to(std::vector<const ConnectionInfoProvider &> & infoProviders);

    VerbsConnection& operator [](int idx);
    const VerbsConnection & operator [](int idx) const;


    size_t sum( size_t local_value, size_t target_rank);
    void* gather(const void* local_value, size_t length_per_connection, size_t target_rank);
    void* gather_dif(const void* local_value, size_t local_length, size_t &gathered_length, size_t target_rank);
    void gather(const void* local_value, void* gathered_values, size_t length_per_connection, size_t target_rank);
    void gather_dif(const void* local_value, size_t local_length, void* gathered_values, size_t &gathered_length, size_t target_rank);
    void* all_gather(const void* local_value, size_t length_per_connection);
    void all_gather(const void* local_value, size_t length_per_connection, void * gathered_values);


    void bcast(void * local_value, size_t length, size_t sender);
    void barrier_all();

private:
    void gather_ex(const void* local_value, size_t* local_lengths, void* gathered_values, size_t target_rank);

};



