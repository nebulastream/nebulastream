/*
 *     ListNode.h
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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_LISTNODE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_LISTNODE_HPP_

#include <vector>

namespace NES::Runtime::Execution {

class ListNode {
  public:
    /**
     * @brief Creates new bucket with a maximum size.
     * @param maxSize
     */
    ListNode(int maxSize);

    /**
     * @brief Insert element at the end.
     * @param value
     * @param var
     * */
    void addBack(const double& value, const double& var);

    /**
     * @brief Drop first n elements.
     * @param n number of elements
     */
    void dropFront(int n = 1);

    const int maxSize;
    int size;
    std::vector<double> sum, variance;
    ListNode* next;
    ListNode* prev;
};
} // namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_LISTNODE_HPP_

