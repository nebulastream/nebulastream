//
// Created by Toso, Lorenzo on 2018-12-13.
//

#pragma once

#include "Debug.h"
#include <queue>
#include <atomic>
#include <mutex>
#include <zconf.h>
#include <cstring>

template <class T>
class SynchronizedBlockwiseQueue {
public:
private:
    std::deque<T> queue;
    std::atomic<bool> is_empty;
    std::mutex m;

public:
    SynchronizedBlockwiseQueue();
    SynchronizedBlockwiseQueue(const SynchronizedBlockwiseQueue<T> & copy) = delete;
    SynchronizedBlockwiseQueue(SynchronizedBlockwiseQueue<T> && copy) noexcept;
    virtual ~SynchronizedBlockwiseQueue() = default;

    void push(T && elements);
    void push_back(T && elements);
    //void push(T* elements, size_t count);
    T pop();

    bool empty();
    size_t size();
};

/*
template<class T>
void SynchronizedBlockwiseQueue<T>::push(T* elements, size_t count) {
    if(count == 0)
        return;
    std::vector<T> new_elements(count);
    memcpy(new_elements.data(), elements, count*sizeof(T));
    push(std::move(new_elements));
}*/

template<class T>
void SynchronizedBlockwiseQueue<T>::push(T && elements) {
    m.lock();
    queue.emplace_back(std::move(elements));
    is_empty = false;
    //TRACE2("Pushed successfully. New queue length is %lu\n", queue.size());
    m.unlock();
}


template<class T>
bool SynchronizedBlockwiseQueue<T>::empty() {
    return is_empty;
}

template<class T>
size_t SynchronizedBlockwiseQueue<T>::size() {
    m.lock();
    auto s = queue.size();
    m.unlock();
    return s;
}


template<class T>
SynchronizedBlockwiseQueue<T>::SynchronizedBlockwiseQueue()
{
    is_empty = true;
}

template<class T>
SynchronizedBlockwiseQueue<T>::SynchronizedBlockwiseQueue(SynchronizedBlockwiseQueue<T> &&copy) noexcept
:is_empty(true) {
    this->queue = std::move(copy.queue);
    this->is_empty = copy.is_empty.load();
    //this->m = std::move(copy.m);


}

template<class T>
T SynchronizedBlockwiseQueue<T>::pop() {
    std::lock_guard<std::mutex> l(m);
    if(is_empty)
        return std::move(T());
    auto block = queue.front();
    queue.pop_front();
    if(queue.empty())
        is_empty = true;
    //TRACE2("Popped successfully. New queue length is %lu\n", queue.size());
    return std::move(block);
}

template<class T>
void SynchronizedBlockwiseQueue<T>::push_back(T &&elements) {
    push(std::move(elements));
}
