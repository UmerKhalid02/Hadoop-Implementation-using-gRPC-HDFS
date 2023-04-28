#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <string>
#include <stdio.h>
#include <vector>
#include <iomanip>
#include <chrono>
#include <fstream>
#include <sstream>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/masterslave.grpc.pb.h"
#else
#include "masterslave.grpc.pb.h"
#endif

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using masterslave::MasterSlave;
using masterslave::Request;
using masterslave::Response;
using masterslave::TRequest;
using masterslave::TResponse;


struct Slave{
  std::string address = "localhost:";
  bool status;
  bool mapper;  // if mapper = 1, it is mapper else it is reducer
  bool taskCompleted = false;
  bool taskAssigned = false;
  int num_tasks = 0;
  std::vector<std::string> inputs;
};

std::mutex output_mutex;

class MasterSlaveClient {
 public:
  MasterSlaveClient(std::shared_ptr<Channel> channel)
      : stub_(MasterSlave::NewStub(channel)) {}

  bool StatusCheck() {
    // Data we are sending to the server.
    Request request;
    Response response;
    ClientContext context;

    // The actual RPC.
    Status status = stub_->GetData(&context, request, &response);
    
    // Act upon its status.
    if (status.ok()) {
      return true;
    } else {      
      return false;
    }
  }

  std::vector<std::pair<std::string, int>> Map(std::string input){
    std::weak_ptr<MasterSlave::Stub> weakStub = stub_;
    std::vector<std::pair<std::string, int>> result;

    try {
        if (auto stub = weakStub.lock()) {
            
            // Use the stub
            TRequest request;
            TResponse response;
            ClientContext context;

            request.set_data(input);
            Status status = stub->Map(&context, request, &response);

            if (status.ok()) {
              for (int i = 0; i < response.word_count_size(); i++) {
                  std::string word = response.word_count(i).key();
                  int count = response.word_count(i).value();
                  result.push_back(std::make_pair(word, count));
              }
            } else {
                std::cout << "RPC failed" << std::endl;
            }
        } else {
            std::cout << "Shared Pointer has expired\n";
        }
        return result;
    } catch (const std::bad_weak_ptr& e) {
        std::cout << "Shared Pointer has expired\n";
        return result;
    }
  }

  std::vector<std::pair<std::string, int>> Reduce(std::string input){
    std::weak_ptr<MasterSlave::Stub> weakStub = stub_;
    std::vector<std::pair<std::string, int>> result;

    try {
        if (auto stub = weakStub.lock()) {
            
            // Use the stub
            TRequest request;
            TResponse response;
            ClientContext context;

            request.set_data(input);
            Status status = stub->Reduce(&context, request, &response);

            if (status.ok()) {
              for (int i = 0; i < response.word_count_size(); i++) {
                  std::string word = response.word_count(i).key();
                  int count = response.word_count(i).value();
                  result.push_back(std::make_pair(word, count));
              }
            } else {
                std::cout << "RPC failed" << std::endl;
            }
        } else {
            std::cout << "Shared Pointer has expired\n";
        }
        return result;
    } catch (const std::bad_weak_ptr& e) {
        std::cout << "Shared Pointer has expired\n";
        return result;
    }
  }

 private:
  std::shared_ptr<MasterSlave::Stub> stub_;
};

void loadFile(std::vector<std::string>& words)
{
  std::ifstream rdr("input.txt");
  std::string s;

  do{
    rdr >> s;
    words.push_back(s);
  }while(!rdr.eof());
}

void splitInput(std::vector<std::string> words, std::vector<std::string>& input, int num_slaves)
{
  std::string str = "";
  for(int i = 0; i < words.size()/2; i++){
    str += words[i];
    str += " ";
  }
  input.push_back(str);
  str = "";
  std::cout << "Halfway There\n";

  for(int i = words.size()/2; i < words.size(); i++){
    str += words[i];
    str += " ";
  }
  input.push_back(str);
}

void initializeSlaves(std::vector<Slave>& slaves, std::vector<std::string> inputs)
{
  slaves[0].address += "50051";
  slaves[1].address += "50052";
  slaves[2].address += "50053";
  slaves[3].address += "50054";

  slaves[0].mapper = 1; // mapper
  slaves[1].mapper = 1; // mapper
  slaves[2].mapper = 0; // reducer
  slaves[3].mapper = 0; // reducer

  // keeps a copy of other mapper input
  slaves[0].inputs = {inputs[0], inputs[1]};
  slaves[0].taskAssigned = true;
  slaves[0].num_tasks += 1;
  slaves[1].inputs = {inputs[1], inputs[0]};
  slaves[1].taskAssigned = true;
  slaves[1].num_tasks += 1;
}

std::string SlaveStatus(std::vector<Slave>& slaves, bool response, int i)
{
  std::string message = "";
  std::string slaveNo = std::to_string(i + 1);
  if(response && slaves[i].mapper && slaves[i].taskAssigned){
    message = "Slave " + slaveNo + " is performing map task";
  }
  else if((response && slaves[i].mapper) || (response && !slaves[i].mapper)){
    message = "Slave " + slaveNo + ": Responsive - currently online";
  }
  else if (!response) {
    slaves[i].status = false;
    message = "Slave " + slaveNo + ": Unresponsive - currently offline";
  }

  return message;
}

// Map Output Splitting
std::vector<std::string> GetWords(std::vector<std::vector<std::pair<std::string, int>>> input)
{
  std::vector<std::string> words;

  for(int i = 0; i < input.size(); i++){
    for(int j = 0; j < input[i].size(); j++){
      words.push_back(input[i][j].first);
    }
  }

  return words;
}

std::vector<std::string> SortKeys(std::vector<std::string> keys)
{
  std::sort(std::begin(keys), std::end(keys));
  return keys;
}

void ReducerInputs(std::vector<std::string> &inputs, std::vector<std::string> keys, int split)
{
  std::string s = "";
  int j = 0;
  for(int i = 0; i < split; i++){
    s += keys[i];
    s += " ";

    if(i == split - 1){
      while((split + j) < keys.size() && keys[i+j] == keys[split+j]){
        s += keys[split+j];
        s += " ";
        j++;
      }
    }
  }

  inputs.push_back(s);
  s = "";
  for(int i = split + j; i < keys.size(); i++){
    s += keys[i];
    s += " ";
  }
  inputs.push_back(s);
}

int main(int argc, char** argv) { 
  int controlInterval = 12;
  int timeoutInterval = controlInterval/3;

  std::vector<Slave> slaves(4);
  std::vector<std::string> inputs{"a a a b c a d d s a", "z x y s a a b l m m n z"};
  
  // std::vector<std::string> inputs;
  // std::vector<std::string> words;
  // std::cout << "Loading File\n";
  // loadFile(words);
  // std::cout << "Splitting Input\n";
  // splitInput(words, inputs, 2);

  initializeSlaves(slaves, inputs);
  

  std::vector<MasterSlaveClient> _slaves;
  std::vector<std::thread> threads;
  std::vector<std::vector<std::pair<std::string, int>>> output;

  // create a client for each slave
  for (int i = 0; i < 4; i++) {
    _slaves.emplace_back(grpc::CreateChannel(slaves[i].address, grpc::InsecureChannelCredentials()));
  }


  int i;
  
  while(true){
    system("clear");

    for(int k = 0; k < slaves.size(); k++){
      if(slaves[k].mapper){
        std::cout << "Slave " << k+1 << " assigned task: ";
        std::cout << slaves[k].taskAssigned << std::endl;
      }
    }
    
    std::cout << std::endl;

    jump:
    i = 0;
    while(i < 4){
      bool response;

      // loop for timeout interval - (checks if a slave becomes online in 4 seconds otherwise it will be marked unresponsive)
      for (auto start = std::chrono::steady_clock::now(), now = start; now < start + std::chrono::seconds{timeoutInterval}; now = std::chrono::steady_clock::now()) {
        response = _slaves[i].StatusCheck();
        if(response){
          slaves[i].status = 1;

          if(slaves[i].mapper && slaves[i].taskAssigned && slaves[i].inputs.size() > 0){
            
            slaves[i].taskAssigned = false;
            slaves[i].num_tasks -= 1;

            std::string input = slaves[i].inputs[0];
            // MasterSlaveClient& s = _slaves[i];
            //threads.emplace_back([&s, input] () { s.Map(input); });

            // performing map tasks on threads and storing results in output
            threads.emplace_back([&slaves, &output, i](std::string input) {
                std::vector<std::pair<std::string, int>> local_output;
                MasterSlaveClient s = grpc::CreateChannel(slaves[i].address, grpc::InsecureChannelCredentials());
                local_output = s.Map(input);
                std::lock_guard<std::mutex> lock(output_mutex);
                output.push_back(local_output);
            }, input);
          }
           
          break;
        } else {
          MasterSlaveClient s = grpc::CreateChannel(slaves[i].address, grpc::InsecureChannelCredentials());
          _slaves[i] = s;
        }
      }

      if(!response && slaves[i].mapper){
        if(i > 0 && slaves[i-1].status == 1 && slaves[i].num_tasks > 0){
          slaves[i-1].num_tasks += 1;
          slaves[i-1].taskAssigned = true;
          slaves[i].taskAssigned = false;
          slaves[i].num_tasks -= 1;
        }
      }
      else if(response && slaves[i].mapper){
        if(i > 0 && slaves[i-1].status == 0 && slaves[i-1].num_tasks > 0){
          slaves[i].num_tasks += 1;
          slaves[i].taskAssigned = true;
          slaves[i-1].taskAssigned = false;
          slaves[i-1].num_tasks -= 1;
        }
      }

      // status checks
      std::string message = SlaveStatus(slaves, response, i);
      std::cout << message << std::endl;
      i++;
    }
    std::cout << std::endl;
    
    for (auto& thread : threads) {
      if (thread.joinable())
        thread.join();
    }

    bool redo = false;
    for (int i = 0; i < slaves.size(); i++){
      if(slaves[i].mapper){
        if(slaves[i].status == 1 && slaves[i].inputs.size() > 0){
          std::cout << "Slave " << i+1 << " successfully completed its task" << std::endl;
          slaves[i].inputs.erase(slaves[i].inputs.begin(), slaves[i].inputs.begin() + 1);
          
          if(slaves[i].num_tasks == 0){
            slaves[i].inputs.clear();  
          }
        } else {
          if(slaves[i].inputs.size() > 0){
            std::cout << "Slave " << i+1 << " failed to complete its task" << std::endl;
            slaves[i].inputs.clear();
            redo = true;
          }
        }
      }
    }

    if(redo){
      system("clear");
      for(int k = 0; k < slaves.size(); k++){
        if(slaves[k].mapper){
          std::cout << "Slave " << k+1 << " assigned task(s): ";
          std::cout << slaves[k].taskAssigned << std::endl;
        }
      }
      goto jump;
    }

    // for 2 slaves (will be changed later)
    std::vector<std::string> keys = GetWords(output);
    
    // sorted keys
    keys = SortKeys(keys);

    // splitting the keys for reducer slaves (splitting by 2 because reducers are 2)
    int split = keys.size() / 2;

    inputs.resize(0);
    output.resize(0);

    // forming reducer inputs for each reducer
    ReducerInputs(inputs, keys, split);

    i = 2;
    int j = 0;

    std::vector<std::vector<std::pair<std::string, int>>> output2;
    while(i < 4){
      std::vector<std::pair<std::string, int>> local_output;
      MasterSlaveClient &s = _slaves[i];
      std::string input = inputs[j];
      local_output = s.Reduce(input);
      output.push_back(local_output);
      i++;
      j++;
    }

    std::vector<std::pair<std::string, int>> kv;
    for(int i = 0; i < output.size(); i++){
      for(int j = 0; j < output[i].size(); j++){
        // std::cout << "(" << output[i][j].first << ", " << output[i][j].second << ")\n";
        std::pair<std::string, int> p;
        p.first = output[i][j].first;
        p.second = output[i][j].second;
        kv.push_back(p);
      }
    }

    // sorting final result
    std::sort(kv.begin(), kv.end(), [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
        return a.second > b.second;
    });
    
    int n;
    std::cout << "Enter number: ";
    std::cin >> n;

    if(n < kv.size()){
      for(int i = 0; i < n; i++){
        std::cout << kv[i].first << ", " << kv[i].second << "\n";
      }
    }


    break;

    // sleep(5);

    // // pauses its execution for 3 seconds then checks again if the slaves are alive or not
    // int secs = 3;
    // while(secs >= 0){
    //   system("clear");
    //   std::cout << "Next heartbeat call will be in " << std::setfill('0') << std::setw(2) << secs << " seconds\n";
    //   secs--; 
    //   sleep(1);
    // }
  }  
  
  return 0;
}