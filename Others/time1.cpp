#include <bits/stdc++.h>
#include <fstream>
#include <sys/time.h>
#include <unistd.h>
#include <thread>
#include "zmq.hpp"
using namespace std;
int a = 0;
uint64_t n, kSleepTime;
vector<string> ips;
void go(string ip){

    zmq::context_t context(1);
    zmq::socket_t socket_send(context, ZMQ_PUSH);
    socket_send.connect("tcp://" + ip);
		zmq::message_t msg(2);
		memcpy(msg.data(), "11", 2);
        socket_send.send(msg);
}

int main(){

    ifstream ifs;
    ifs.open("text.txt", ios::in);
    if(!ifs.is_open()){
        cout << "open file failed" << endl;
        return 0;
    }

    string buf;

    string port;

    getline(ifs, buf);
    n = stoull(buf);
    getline(ifs, buf);
    kSleepTime = stoull(buf);
    for(int i = 0; i < n; i++){
        getline(ifs, buf);
        ips.push_back(buf);
    }
    getline(ifs, buf);
	port = buf;
    for(int i = 0; i < n; i++){
        ips[i] = ips[i] +  ":" + port;
        cout << ips[i] << endl;

    }

    thread t[1000];
    for(int i = 0; i < n; i++){
        t[i] = thread(go, ips[i]);
    }
    usleep(1000000);
    a = 1;

    for(int i = 0; i < n; i ++){
        t[i].join();
    }
    cout << "end" << endl;
}
