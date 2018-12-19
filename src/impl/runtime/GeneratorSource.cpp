/*
 * GeneratorSource.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */


#include "../include/GeneratorSource.h"


template <typename F>
TupleBuffer GeneratorSource<F>::receiveData(){
    std::cout << "produced " << generated_tuples
              << " tuples from total " << num_tuples_to_process
              << " tuples " << std::endl;

    if(generated_tuples<num_tuples_to_process){
        TupleBuffer buf = functor(generated_tuples, num_tuples_to_process);
        generated_tuples+=buf.num_tuples;
        return buf;
    }
    /* job done, quit this source */
    //std::this_thread::sleep_for(std::chrono::seconds(1)); //nanoseconds(100000));
    return TupleBuffer(NULL,0,0,0);
}
