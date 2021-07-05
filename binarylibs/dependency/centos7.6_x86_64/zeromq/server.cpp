#include <iostream>
#include <zmq.hpp>
#include <string>
using namespace std;
//#include "message.pb.h"
//using merge::MergeRequest;
int main()
{
    zmq::context_t context(1);
    //MergeRequest req;
        //zmq_ctx_set(&context, ZMQ_IO_THREADS, 3);
    // 负责pull　response
    zmq::socket_t response_puller(context, ZMQ_PULL);
    //response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    response_puller.bind("tcp://*:5055");//bind监听端口
    zmq::message_t message;
    for(;;){
        const auto ret = response_puller.recv(&message);
        string res=string(static_cast<const char*>(message.data()), message.size());
        //req.ParseFromString(res);
        if (!ret)
            return 1;
        cout << "I Got: " << res << " messages" << std::endl<<std::endl;
    }
    return 0;
}
