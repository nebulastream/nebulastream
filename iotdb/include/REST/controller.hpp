#pragma once

#include <cpprest/http_msg.h>

using namespace web;
using namespace http;

namespace iotdb {

   /*!
    * Dispatcher class represents the basic interface for a 
    * web serivce handler.
    */
    class Controller {
    public: 
        virtual void handleGet(http_request message) = 0;
        virtual void handlePut(http_request message) = 0;
        virtual void handlePost(http_request message) = 0;
        virtual void handleDelete(http_request message) = 0;
        virtual void handlePatch(http_request messge) = 0;
        virtual void handleHead(http_request message) = 0;
        virtual void handleOptions(http_request message) = 0;
        virtual void handleTrace(http_request message) = 0;
        virtual void handleConnect(http_request message) = 0;
        virtual void handleMerge(http_request message) = 0;
    };
}