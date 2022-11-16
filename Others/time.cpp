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
    struct timeval start_time, now_time;
    uint64_t start_time_ll, now_time_ll;
    gettimeofday(&start_time, NULL);
    start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
    uint64_t cnt = 0;
    uint64_t sleep_time = kSleepTime;
    
    while(!a);

    while(true){
		zmq::message_t msg(2);
		memcpy(msg.data(), "11", 2);
        socket_send.send(msg);
        gettimeofday(&now_time, NULL);
        now_time_ll = now_time.tv_sec * 1000000 + now_time.tv_usec;
        sleep_time = (now_time_ll - start_time_ll) - (cnt * kSleepTime);
        if(sleep_time < kSleepTime){
            usleep(kSleepTime - sleep_time);
        }
cout <<ip <<  " " << sleep_time << " " << kSleepTime << " " << kSleepTime - sleep_time << " " << now_time_ll << endl;
cnt++;
    }
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

    thread t[10];
    for(int i = 0; i < n; i++){
        t[i] = thread(go, ips[i]);
    }
    usleep(3000000);
    a = 1;

    for(int i = 0; i < n; i ++){
        t[i].join();
    }
    cout << "end" << endl;
}
