//
// Created by Toso, Lorenzo on 2019-03-11.
//
#ifndef MAMPI_SENDRECEIVETUPLETHREAD_H
#define MAMPI_SENDRECEIVETUPLETHREAD_H
#pragma once

#include "VerbsConnection.h"
#include "SynchronizedBlockwiseQueue.h"
#include <iterator>
#include "AbstractDataExchangeOperator.h"

class SendReceiveOperator : public AbstractDataExchangeOperator {

public:
    explicit SendReceiveOperator(ConnectionCollection &connections)
            : AbstractDataExchangeOperator(connections){}


protected:
    void receive_tuples_from(size_t target_rank, const std::function<void(const StructuredTupleBuffer &, std::atomic_char*)> & tuple_processor) override;

    void send_tuples_to_all (const ProbeTable * probeTable, const Limits & limits) override;

    void send_matching_tuples_to (size_t target_rank, const ProbeTable * probeTable, const Limits & limits, const std::function<bool(size_t)> & matcher) override;

};


#endif //MAMPI_JOINPARAMETERS_H
