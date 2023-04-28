#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <sstream>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/masterslave.grpc.pb.h"
#else
#include "masterslave.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using masterslave::MasterSlave;
using masterslave::Request;
using masterslave::Response;
using masterslave::TRequest;
using masterslave::TResponse;


// Logic and data behind the server's behavior.
class MasterSlaveServiceImpl final : public MasterSlave::Service {
  Status GetData(ServerContext* context, const Request* request, Response* response) override {
    return Status::OK;
  }

  Status Map(ServerContext* context, const TRequest* request, TResponse* response) override {
    system("clear");
    std::string input = request->data();
    std::vector<std::string> words;
    std::stringstream ss(input);
    std::string line;
    while(std::getline(ss, line, ' ')){
        words.push_back(line);
    }

    for (auto word : words) {
      TResponse::KeyValuePair* kv = response->add_word_count();
      kv->set_key(word);
      kv->set_value(1);

      // to be deleted afterwards
      std::cout << "Key: " << word << ", Value: " << 1 << std::endl;
    }
    std::cout << "\nTask Successful\n>>> Slave 1 listening on 0.0.0.0:50051\n\n";
    return Status::OK;
  }

  Status Reduce(ServerContext* context, const TRequest* request, TResponse* response) override {
    system("clear");
    std::string input = request->data();
    std::vector<std::string> words;
    std::stringstream ss(input);
    std::string line;
    while(std::getline(ss, line, ' ')){
        words.push_back(line);
    }

    int count = 1;
    for(int i = 0; i < words.size(); i++){
      if((i < words.size()-1) && words[i] == words[i+1])
        count++;
      else {
        TResponse::KeyValuePair* kv = response->add_word_count();
        kv->set_key(words[i]);
        kv->set_value(count);
        std::cout << "Key: " << words[i] << ", Value: " << count << std::endl;
        count = 1;
      }
    }

    std::cout << "\nTask Successful\n>>> Slave 1 listening on 0.0.0.0:50051\n\n";

    return Status::OK;
  }

};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  MasterSlaveServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case, it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Slave 1 listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
