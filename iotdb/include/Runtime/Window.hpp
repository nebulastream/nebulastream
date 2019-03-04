/*
 * Window.hpp
 *
 *  Created on: Mar 4, 2019
 *      Author: zeuchste
 */

#ifndef INCLUDE_RUNTIME_WINDOW_HPP_
#define INCLUDE_RUNTIME_WINDOW_HPP_
#include <memory>

namespace iotdb{
class Window;
typedef std::shared_ptr<Window> WindowPtr;

class Window {
public:

	Window();
	void setup(){};
	void print(){};
	void shutdown(){};

private:
};
const WindowPtr createTestWindow();


}



#endif /* INCLUDE_RUNTIME_WINDOW_HPP_ */
