/*
 *     List.h
 *      
 *      Copyright (C) 2008 Universitat Politècnica de Catalunya
 *      @author Albert Bifet - Ricard Gavaldà (abifet,gavalda@lsi.upc.edu)
 *           
 *      This program is free software; you can redistribute it and/or modify
 *      it under the terms of the GNU General Public License as published by
 *      the Free Software Foundation; either version 2 of the License, or
 *      (at your option) any later version.
 *      
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU General Public License for more details.
 *      
 *      You should have received a copy of the GNU General Public License
 *      along with this program; if not, write to the Free Software
 *      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 *      MA 02110-1301, USA.
 */

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_LIST_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_LIST_HPP_

#include <Execution/StatisticsCollector/ChangeDetectors/Adwin/ListNode.hpp>

namespace NES::Runtime::Execution {

class List {
 public:
    /**
     * @brief Initialize empty list of buckets.
     * @param maxBucketSize
     */
    List(int maxBucketSize);

    ~List();

    /**
     * @brief Add new node at the end of the list.
     */
    void addToTail() ;

    /**
     * @brief Remove last node in list.
     */
    void removeFromTail();

    const int maxBucketSize;
    int count;
    ListNode* head;
    ListNode* tail;

 private:
    /**
     * @brief Add new node to beginning of list.
     */
    void addToHead();

    /**
     * @brief Remove first node from list.
     */
    void removeFromHead();

};
} // namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_LISTNODE_HPP_
