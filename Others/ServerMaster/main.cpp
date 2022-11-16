#include <bits/stdc++.h>
#include <fstream>
#include <sys/time.h>
#include <unistd.h>
#include <thread>
#include "zmq.hpp"
#include "massage.pb.h"
#include "blocking_concurrent_queue.hpp"
using namespace std;
std::vector<uint64_t> server_physical_epoch, server_logical_epoch, server_commit_rest_time_count,
        server_commit_lack_time_count, server_delay_time, server_confirm;
std::vector<std::string> server_ip;

std::atomic<uint64_t> sned_count;
uint64_t server_id, server_count,
        max_get_physical_epoch, max_delay_time, delay_time_count, delay_time_tot, //server传递过来的
        epoch_start_time, start_physical_epoch, epoch_size,//正在运行的
        now_physical_epoch,
        new_start_physical_epoch, new_epoch_start_time, new_epoch_size,//即将应用的
        current_max_physical_epoch_time, current_min_physical_epoch_time, end;
bool reset_epoch_flag, lack_time_flag, epoch_send_thread_init = false;

[[noreturn]] void EpochMessageSendThreadMain(string ip){
    zmq::context_t context(1);
    zmq::socket_t socket_send(context, ZMQ_PUSH);
    socket_send.connect("tcp://" + ip);
    struct timeval start_time, now_time;
    uint64_t start_time_ll, now_time_ll;
    gettimeofday(&start_time, NULL);
    start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;
    uint64_t cnt = 0;
    uint64_t sleep_time = epoch_size;

    while(!epoch_send_thread_init);

    for(;;){
        zmq::message_t msg(2);
        memcpy(msg.data(), "11", 2);
        socket_send.send(msg);
        if(start_physical_epoch + cnt == new_start_physical_epoch){//更新到新的epoch size
            start_physical_epoch = new_start_physical_epoch;
            cnt = 0;
            gettimeofday(&now_time, NULL);
            start_time_ll = now_time.tv_sec * 1000000 + now_time.tv_usec;
        }
        gettimeofday(&now_time, NULL);
        now_time_ll = now_time.tv_sec * 1000000 + now_time.tv_usec;
        sleep_time = (now_time_ll - start_time_ll) - (cnt * epoch_size);
        if(sleep_time < epoch_size){
            usleep(epoch_size - sleep_time);
        }
        cout <<ip <<  " " << sleep_time << " " << epoch_size << " " << epoch_size - sleep_time
            << " " << now_time_ll << endl;
        cnt++;

    }
    return ;
}

struct Receive{
    std::unique_ptr<zmq::message_t> message_ptr;
    uint64_t receive_time;
    Receive(uint64_t rt, std::unique_ptr<zmq::message_t> &&mp):receive_time(rt), message_ptr(std::move(mp)){}
    Receive(){}
};

moodycamel::BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>> message_send_pool;
moodycamel::BlockingConcurrentQueue<std::unique_ptr<Receive>> message_receive_pool;

[[noreturn]] void EpochMessageSendThreadMain(uint64_t id){
    int num = 0;
    zmq::context_t context(1);
    zmq::socket_t socket_send(context, ZMQ_PUSH);
    std::unique_ptr<zmq::message_t> params;
    while(true){
        while(message_send_pool.try_dequeue(params)){
            for(; num < server_ip.size(); num++){
                socket_send.connect("tcp://" + server_ip[num] + ":5547");
            }
            socket_send.send((*params));
        }
        usleep(50);
    }
    return ;
}

void EpochMessageListenThreadMain(uint64_t id){//master监听端口中来自所有server的信息（包括新增）
    zmq::context_t context(1);
    zmq::socket_t socket_listen(context, ZMQ_PULL);
    socket_listen.bind("tcp://*:5547");
    struct timeval time;
    uint64_t receive_time;
    for(;;) {
        std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
        socket_listen.recv(&(*message_ptr));
        gettimeofday(&time, NULL);
        receive_time = time.tv_sec * 1000000 + time.tv_usec;
        if(!message_receive_pool.enqueue(std::move(std::make_unique<Receive>(receive_time, std::move(message_ptr)))))
            assert(false);
    }
    return ;
}


uint64_t now_to_us(){
    return std::chrono::duration_cast<std::chrono::microseconds>
                (std::chrono::system_clock::now().time_since_epoch()).count();
}

void GenerateServerSendMessage(uint32_t type, uint32_t server_id, std::string ip, uint64_t physical_epoch,
                               uint64_t logical_epoch,uint64_t epoch_size, uint64_t commit_time, uint64_t new_server_id,
                               std::string new_server_ip){
    std::unique_ptr<merge::ServerMessage> server_message_ptr = std::make_unique<merge::ServerMessage>();
    server_message_ptr->set_type(type);
    server_message_ptr->set_serverid(server_id);
    server_message_ptr->set_ip(ip);
    server_message_ptr->set_physicalepoch(physical_epoch);
    server_message_ptr->set_logicalepoch(logical_epoch);
    server_message_ptr->set_committime(commit_time);
    server_message_ptr->set_send_time(now_to_us);
    server_message_ptr->set_new_server_id(new_server_id);
    server_message_ptr->set_new_server_ip(new_server_ip);

    std::unique_ptr<std::string> send_message_string_ptr = std::make_unique<std::string>();
    server_message_ptr->SerializeToString(&(*send_message_string_ptr));
    std::unique_ptr<zmq::message_t> send_message_ptr = std::make_unique<zmq::message_t>(send_message_string_ptr->size());
    memcpy(send_message_ptr->data(), send_message_string_ptr->c_str(), send_message_string_ptr->size());
    message_send_pool.enqueue(std::move(send_message_ptr));
    return ;
}

void ServerMessageRecord(uint64_t id, uint64_t get_physical_epoch, uint64_t get_logical_epoch, uint64_t commit_time,
                         uint64_t delay_time){
    delay_time_tot += delay_time;
    delay_time_count ++;
    max_get_physical_epoch = get_physical_epoch > max_get_physical_epoch ? get_physical_epoch : max_get_physical_epoch;
    max_delay_time =delay_time > max_delay_time ? delay_time : max_delay_time;

    if(get_physical_epoch > start_physical_epoch + 5){//服务器重新开始新的后epoch size后再开始统计
        server_count = 0;
        reset_epoch_flag = true;
    }
    if(delay_time_count >= 100){
        max_delay_time = static_cast<uint64_t>(delay_time_tot / delay_time_count);
        delay_time_count = 1;
    }
    if(reset_epoch_flag){
        if(get_physical_epoch > get_logical_epoch + 1){
            if( ++ server_commit_lack_time_count[id] > 10){
                lack_time_flag = true;
            }
            server_commit_rest_time_count[id] = 0;
        }
        else{
            if(commit_time + 5000 < epoch_size){
                server_commit_lack_time_count[id] = 0;
                server_commit_rest_time_count[id] ++;
            }
        }
    }
    server_delay_time[id] = delay_time;

    return ;
}

void AdjustEpochSize(){
    if(lack_time_flag == true){// epoch size ++
        for(int i = 0; i < server_ip.size(); i++){
            server_commit_lack_time_count[i] = 0;
            server_commit_rest_time_count[i] = 0;
        }
        new_epoch_size = epoch_size + 5000;// us
        reset_epoch_flag = false;
    }
    else {// epoch size --
        server_count = 0;
        for(int i = 0; i < server_ip.size(); i++){
            if(server_commit_rest_time_count[i] < 50){
                break;
            }
            server_count ++;
        }
        if(server_count >= server_ip.size()){
            if(epoch_size > 5000){
                new_epoch_size = epoch_size + 5000;
            }
            reset_epoch_flag = false;
        }
    }

    if(reset_epoch_flag == false){//确定new_start_physical_epoch
        new_start_physical_epoch = max_get_physical_epoch + (max_delay_time/epoch_size) + 5;
    }
    //发送给server由server处理
    // GenerateServerSendMessage(1, 0, "", new_start_physical_epoch, 0, new_epoch_size, 0, 0, "");

    // 更改epoch 消息线程发送信息
    // EpochMessageSendThreadMain(string ip)
//    if(start_physical_epoch + cnt == new_start_physical_epoch){//更新到新的epoch size
//        start_physical_epoch = new_start_physical_epoch;
//        cnt = 0;
//        gettimeofday(&now_time, NULL);
//        start_time_ll = now_time.tv_sec * 1000000 + now_time.tv_usec;
//    }
}

// // 规定: 如果physical超过logical一定数量（20）后便触发epoch增加
// // 规定: commit 消耗时间比sleeptime小一个减少量（5ms）持续一定数量（20）后便触发epoch增加
// // 规定: 新增节点先向master节点报告 type 0，随后, master节点向server节点发送消息告知epoch大小type 3
// //（server定期发送 以确定是否由server宕机, 使用push pull方式可以在服务端检测)
// //        master汇总后向新增节点和所有server节点发送新增节点信息type 0（其他节点开始向新增节点发送epoch txns）
// message ServerMessage{
//   uint32_t type = 1;//server to master 消息类型 0:新增节点以及新增节点开始时间？ 1:server节点通报自己的情况 运行到了哪个epoch
//                                              //2: server节点反馈 epoch size更新信息 3:server节点返回新增节点信息
//                     //master to server 消息类型 0:新增节点 1:adjust epoch size 2:
//   uint32_t server_id = 2;
//   string ip = 3;
//   uint64_t physical_epoch = 4;//新epochsize开始时间epoch 发送给server进行修改自己的sleeptime或修改发送线程sleeptime
//   uint64_t logical_epoch = 5;
//   uint64_t epoch_size = 6;//新epochsize 或现有的epochsize大小
//   uint64_t commit_time = 7;//提交耗时
//   uint64_t send_time = 8; //用以确定网络时延，决定新epoch size开启时间
//   uint64_t new_server_id = 9;
//   uint64_t new_server_ip = 10;
// }

void EpochMessageMasterThreadMain(std::thread* epoch_send_thread_ids){
    std::unique_ptr<Receive> receive_ptr;
    std::unique_ptr<zmq::message_t> send_message_ptr = std::make_unique<merge::ServerMessage>();;
    std::unique_ptr<zmq::message_t> message_string_ptr;
    std::unique_ptr<merge::ServerMessage> server_message_ptr;

    for(;;){
        while(message_receive_pool.try_dequeue(receive_ptr)){
            message_string_ptr = std::make_unique<std::string>(
                    static_cast<const char*>(receive_ptr->message_ptr->data()), receive_ptr->message_ptr->size());
            server_message_ptr = std::make_unique<merge::ServerMessage>();
            server_message_ptr->ParseFromString(*message_string_ptr);

            switch (server_message_ptr->type())
            {
                case /* constant-expression */ 0:
                    /* code */

                    break;

                case /* constant-expression */ 1:
                    /* code */
                    ServerMessageRecord(server_message_ptr->server_id(), server_message_ptr->physical_epoch(),
                                        server_message_ptr->logical_epoch(), server_message_ptr->commit_time(),
                                        receive_ptr->receive_time - server_message_ptr->send_time());
                    break;

                case /* constant-expression */ 2:
                    /* code */
                    //假设无误 暂不处理
                    break;

                case /* constant-expression */ 3:
                    /* code */
                    //假设无误 暂不处理
                    break;

                default:
                    break;
            }
        }

        //change epoch size
        if(reset_epoch_flag){
            AdjustEpochSize();
        }



    }
}







int main() {
    std::ifstream ifs;
    ifs.open("text.txt", ios::in);
    if(!ifs.is_open()){
        cout << "open file failed" << endl;
        return 0;
    }

    string buf, port;
    getline(ifs, buf);
    server_count = stoull(buf);
    getline(ifs, buf);
    epoch_size = stoull(buf);
    start_physical_epoch = 1;
    new_start_physical_epoch = 0;
    for(int i = 0; i < server_count; i++){
        getline(ifs, buf);
        server_ip.push_back(buf);
    }
    getline(ifs, buf);
    port = buf;
//    for(int i = 0; i < server_count; i++){
//        server_ip[i] = server_ip[i] +  ":" + port;
//        cout << server_ip[i] << endl;
//    }

    std::thread epoch_send_thread_ids[100];
    for(int i = 0; i < server_count; i++){
        epoch_send_thread_ids[i] = std::thread(EpochMessageSendThreadMain, server_ip[i] + ":" + port);
    }
    std::thread message_listen_thread_id = std::thread(EpochMessageListenThreadMain, 0);
    std::thread message_send_thread_id = std::thread(EpochMessageSendThreadMain, 0);
    std::thread master = std::thread(EpochMessageMasterThreadMain, epoch_send_thread_ids);

    usleep(3000000);
    epoch_send_thread_init = true;

    for(int i = 0; i < server_count; i ++){
        epoch_send_thread_ids[i].join();
    }
    cout << "end" << endl;
    return 0;
}
