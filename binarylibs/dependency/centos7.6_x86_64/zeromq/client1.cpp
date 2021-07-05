#include <iostream>
#include <zmq.hpp>
#include <string>
using namespace std;

int main()
{
    zmq::context_t context(1);
    zmq::socket_t request_pusher(context, ZMQ_PUSH);
    request_pusher.connect("tcp://209.199.6.60:5557");
    zmq::message_t msg(4);
    memcpy(msg.data(), "123", 3);
	request_pusher.send(msg);

    return 0;
}
