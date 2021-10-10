// #include <ostream>
// #include <istream>
// #include <iomanip>
// #include <pthread.h>
// #include <cstring>
// #include "message.pb.h"
// #include "postgres.h"
// #include "access/dfs/dfs_query.h"
// #include "access/sysattr.h"
// #include "nodes/parsenodes.h"
// #include "nodes/pg_list.h"
// #include "nodes/nodeFuncs.h"
// #include "nodes/makefuncs.h"
// #include "parser/parse_type.h"
// #include "utils/syscache.h"
// #include "executor/executor.h"
// #include "storage/ipc.h"
// #include "commands/dbcommands.h"
// #include "knl/knl_session.h"

// #include "mot_internal.h"
// #include "row.h"
// #include "log_statistics.h"
// #include "spin_lock.h"
// #include "txn.h"
// #include "table.h"
// #include "utilities.h"
// #include "mot_engine.h"
// #include "sentinel.h"
// #include "txn.h"
// #include "txn_access.h"
// #include "index_factory.h"
// #include "column.h"
// #include "mm_raw_chunk_store.h"
// #include "ext_config_loader.h"
// #include "config_manager.h"
// #include "mot_error.h"
// #include "utilities.h"
// #include "jit_context.h"
// #include "mm_cfg.h"
// #include "jit_statistics.h"
// #include "gaussdb_config_loader.h"
// #include <cstdio> 
// #include <stdio.h>
// #include <iostream>
// #include "string"
// #include "sstream"
// #include <fstream>
// #include <atomic>
// #include <sys/time.h>
// // #include "neu_concurrency_tools/blockingconcurrentqueue.h"
// // #include "neu_concurrency_tools/blocking_mpmc_queue.h"
// // #include "neu_concurrency_tools/ThreadPool.h"
// // #include "epoch_merge.h"
// #include "../../../../fdw_adapter/src/mot_internal.h"//ADDBY NEU
// #include "zmq.hpp"
// #include <sched.h>
// #include "postmaster/postmaster.h"
// #include "epochmessage.h"


// vector<uint64_t> server_physical_epoch, server_logical_epoch, server_commit_rest_time_count, 
//                         server_commit_lack_time_count, server_delay_time, server_confirm, server_ip;
// uint64_t server_id, server_count,//server传递过来的
//             max_get_physical_epoch, max_delay_time, delay_time_count, delay_time_tot,
//             epoch_start_time, start_physical_epoch, epoch_size,//正在运行的
//             new_start_physical_epoch, new_epoch_start_time, new_epoch_size,//即将应用的
//             current_max_physical_epoch_time, current_min_physical_epoch_timeend;
// bool reset_epoch_flag, lack_time_flag;
// // epoch 推进端口 5546
// // message 通信端口 5547 (暂定)
// struct Receive{
//     std::unique_ptr<zmq::message_t> message_ptr;
//     uint64_t receive_time;
//     Receive(uint64_t rt, std::unique_ptr<zmq::message_t> &&mp):receive_time(rt), message_ptr(std::move(mp)){}
//     Receive(){}
// };

// moodycamel::BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>> message_send_pool;
// moodycamel::BlockingConcurrentQueue<std::unique_ptr<Receive>> message_receive_pool;

// void EpochMessageSendThreadMain(uint64_t id){
//     SetCPU();
//     int num = 0;
//     zmq::context_t context(1);
//     // int queue_length = 0;
//     // zmq::socket_t socket_send(context, ZMQ_PUB);
//     // socket_send.setsockopt(ZMQ_SNDHWM, &queue_length, sizeof(queue_length));
//     zmq:socket_t socket_send(context, ZMQ_PUSH);
//     std::unique_ptr<zmq::message_t> params;
//     while(true){
//         while(message_send_pool->try_dequeue(params)){
//             for(; num < server_ip.size(); num++){
//                 socket_send.connect("tcp://" + server_ip[num] + ":5547");
//                 // MOT_LOG_INFO("线程开始工作 MasterMessageSendThread %s", ("tcp://" + server_ip[num] + ":5547").c_str());
//             }
//             socket_send.send(*(params->merge_request_ptr));
//         }
//         usleep(50);
//     }
// }

// void EpochMessageListenThreadMain(uint64_t id){
//     SetCPU();
    
// 	zmq::context_t context(1);
//     // zmq::message_t reply(5);
// 	zmq::socket_t socket_listen(context, ZMQ_PULL);
//     socket_listen.bind("tcp://*:5547"));
//     int queue_length = 0;
//     // zmq::socket_t socket_listen(context, ZMQ_SUB);
//     // socket_listen.setsockopt(ZMQ_SUBSCRIBE, "", 0);
//     // socket_listen.setsockopt(ZMQ_RCVHWM, &queue_length, sizeof(queue_length));
//     // socket_listen.connect("tcp://" +kServerIp + ":" + std::to_string(port));
//     // MOT_LOG_INFO("线程开始工作 MessageListenThread %s", ("tcp://:5547").c_str());
//     // socket_listen.setsockopt(ZMQ_SUBSCRIBE, "", 0);
//     struct timeval time;
//     uint64_t receive_time;
//     for(;;) {
//         std::unique_ptr<zmq::message_t> message_ptr = std::make_unique<zmq::message_t>();
//         socket_listen.recv(&(*message_ptr));
//         gettimeofday(&time, NULL);
//         receive_time = time.tv_sec * 1000000 + time.tv_usec;
//         // socket_listen.send(reply);
//         if(!message_receive_pool.enqueue(std::move(std::make_unique<Receive>(receive_time, std::move(message_ptr))))) assert(false);
//     }
// }



// // // 规定: 如果physical超过logical一定数量（20）后便触发epoch增加
// // // 规定: commit 消耗时间比sleeptime小一个减少量（5ms）持续一定数量（20）后便触发epoch增加
// // // 规定: 新增节点先向master节点报告 type 0，随后, master节点向server节点发送消息告知epoch大小type 3
// // //（server定期发送 以确定是否由server宕机, 使用push pull方式可以在服务端检测)  
// // //        master汇总后向新增节点和所有server节点发送新增节点信息type 0（其他节点开始向新增节点发送epoch txns）
// // message ServerMessage{
// //   uint32_t type = 1;//server to master 消息类型 0:新增节点以及新增节点开始时间？ 1:server节点通报自己的情况 运行到了哪个epoch 
// //                                              //2: server节点反馈 epoch size更新信息 3:server节点返回新增节点信息
// //                     //master to server 消息类型 0:新增节点 1:adjust epoch size 2:
// //   uint32_t server_id = 2;
// //   string ip = 3;
// //   uint64_t physical_epoch = 4;//新epochsize开始时间epoch 发送给server进行修改自己的sleeptime或修改发送线程sleeptime
// //   uint64_t logical_epoch = 5;
// //   uint64_t epoch_size = 6;//新epochsize 或现有的epochsize大小
// //   uint64_t commit_time = 7;//提交耗时
// //   uint64_t send_time = 8; //用以确定网络时延，决定新epoch size开启时间
// //   uint64_t new_server_id = 9;
// //   uint64_t new_server_ip = 10;
// // }

// uint64_t now_to_us(){
//     return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
// }

// void GenerateServerSendMessage(uint32_t type, uint32_t server_id, std::string ip, uint64_t physical_epoch, uint64_t logical_epoch, 
//     uint64_t epoch_size, uint64_t commit_time, uint64_t new_server_id, std::string new_server_ip){
//     std::unique_ptr<merge::ServerMessage> server_message_ptr = std::make_unique<merge::ServerMessage>();
//     server_message_ptr->set_type(type);
//     server_message_ptr->set_serverid(server_id);
//     server_message_ptr->set_ip(ip);
//     server_message_ptr->set_physicalepoch(physical_epoch);
//     server_message_ptr->set_logicalepoch(logical_epoch);
//     server_message_ptr->set_committime(commit_time);
//     server_message_ptr->set_send_time(now_to_us);
//     server_message_ptr->set_new_server_id(new_server_id);
//     server_message_ptr->set_new_server_ip(new_server_ip);

//     std::unique_ptr<std::string> send_message_string_ptr = std:::make_unique<std::string>();
//     server_message_ptr->SerializeToString(&(*send_message_string_ptr));
//     std::unique_ptr<zmq::message_t> send_message_ptr = std::make_unique<zmq::message_t>(send_message_string_ptr->size());
//     memcpy(send_message_ptr->data(), send_message_string_ptr.c_str(), send_message_string_ptr.size());
//     message_send_pool.enqueue(std::move(send_message_ptr));
//     return ;
// }

// void ServerMessageRecord(uint64_t id, uint64_t get_physical_epoch, uint64_t get_logical_epoch, uint64_t delay_time){
//     delay_time_tot += delay_time;
//     delay_time_count ++;
//     max_get_physical_epoch = get_physical_epoch > max_get_physical_epoch ? get_physical_epoch : max_get_physical_epoch;
//     max_delay_time =delay_time > max_delay_time ? delay_time : max_delay_time;
    
//     if(get_physical_epoch > start_physical_epoch + 5){//服务器重新开始新的后epoch size后再开始统计
//         server_count = 0;
//         reset_epoch_flag = true;
//     }
//     if(delay_time_count >= 100){
//         max_delay_time = static_cast<uint64_t>(delay_time_tot / delay_time_count);
//         delay_time_count = 1;
//     }
//     if(reset_epoch_flag){
//         if(get_physical_epoch > get_logical_epoch + 1){
//             if( ++ server_commit_lack_time_count[id] > 10){
//                 lack_time_flag = true;
//             }
//             server_commit_rest_time_count[id] = 0;
//         }
//         else{
//             if(server_message_ptr->committime + 5000 < epoch_size){
//                 server_commit_lack_time_count[id] = 0;
//                 server_commit_rest_time_count[id] ++;
//             }
//         }
//     }
//     server_delay_time[id] = delay_time;

//     return ;
// }

// void AdjustEpochSize(){
//     if(lack_time_flag == true){// epoch size ++
//         for(int i = 0; i < kServerIp.size(); i++){
//             server_commit_lack_time_count[id] = 0;
//             server_commit_rest_time_count[id] = 0;
//         }
//         new_epoch_size = epoch_size + 5000;// us
//         reset_epoch_flag = false;
//     }
//     else {// epoch size --
//         server_count = 0;
//         for(int i = 0; i < kServerIp.size(); i++){
//             if(server_commit_rest_time_count[i] < 50){
//                 break;
//             }
//             server_count ++;
//         }
//         if(server_count >= kServerIp.size()){
//             if(epoch_size > 5000){
//                 new_epoch_size = epoch_size + 5000;
//             }
//             reset_epoch_flag = false;
//         }
//     }

//     if(reset_epoch_flag == false){//确定new_start_physical_epoch
//         new_start_physical_epoch = max_get_physical_epoch + (max_delay_time/epoch_size) + 5;
//         message_ptr->set_type(4);
//     }

//     //发送给server由server处理
//     // GenerateServerSendMessage(1, 0, "", new_start_physical_epoch, 0, new_epoch_size, 0, 0, "");

//     // 更改epoch 消息线程发送信息

// }



// void EpochMessageMasterThreadMain(){
//     SetCPU();
//     std::unique_ptr<Receive> receive_ptr;
//     std::unique_ptr<zmq::message_t> send_message_ptr = std::make_unique<merge::ServerMessage>();;
//     std::unique_ptr<zmq::message_t> message_string_ptr;
//     std::unique_ptr<merge::ServerMessage> send_server_message_ptr;
    
//     for(;;){
//         while(message_receive_pool.try_dequeue(receive_ptr)){
//             message_string_ptr = std::make_unique<std::string>(
//                     static_cast<const char*>(receive_ptr->message_ptr->data()), receive_ptr->message_ptr->size());
//             server_message_ptr = std::make_unique<merge::ServerMessage>();
//             server_message_ptr->ParseFromString(*message_string_ptr);

//             switch (server_message_ptr->type())
//             {
//             case /* constant-expression */ 0:
//                 /* code */

//                 break;
            
//             case /* constant-expression */ 1:
//                 /* code */
//                 ServerMessageRecord(server_message_ptr->server_id(), server_message_ptr->physical_epoch(), server_message_ptr->logical_epoch()
//                     receive_ptr->receive_time - server_message_ptr->send_time());
//                 break;

//             case /* constant-expression */ 2:
//                 /* code */
//                 //假设无误 暂不处理
//                 break;

//             case /* constant-expression */ 3:
//                 /* code */
//                 //假设无误 暂不处理
//                 break;
            
//             default:
//                 break;
//             }
//         }

//         //change epoch size
//         if(reset_epoch_flag){
//             AdjustEpochSize();
//         }

//     }
// }















