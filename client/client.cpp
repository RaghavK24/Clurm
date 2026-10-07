#include "jobs/job.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <thread>

#include <cstdint>

#include "../common/base64.hpp"

using namespace std;

string read_payload(int socket_fd);
bool send_payload(int socket_fd, const string& payload);

namespace {

// pick a callback port from the pid
int create_callback_port() {
    return 10000 + (getpid() % 1000);
}

// add client_callback_port to the json payload
string attach_port(const string& payload, int callback_port) {
    // position of the closing brace
    size_t close_brace = payload.rfind('}');
    if (close_brace == string::npos) {
        return payload;
    }

    string updated = payload;
    // add the field before it
    updated.insert(close_brace, ",\"client_callback_port\":" + to_string(callback_port));
    return updated;
}

// read the executable, base64 encode it and add it to the json payload
string attach_upload(const string& payload, const string& executable_path) {
    if (executable_path.empty()) {
        return payload;
    }

    // read the executable
    ifstream in(executable_path.c_str(), ios::binary);
    if (!in) {
        cerr << "Unable to open executable for upload at " << executable_path << endl;
        return payload;
    }

    // convert to base64 encoding
    vector<unsigned char> bytes((istreambuf_iterator<char>(in)), istreambuf_iterator<char>());
    string b64 = common::base64_encode(bytes);

    string name = executable_path;
    size_t sep = name.find_last_of('/');
    if (sep != string::npos) {
        name = name.substr(sep + 1);
    }

    // position of the closing brace
    size_t close_brace = payload.rfind('}');
    if (close_brace == string::npos) {
        return payload;
    }

    string updated = payload;
    // add the fields before it
    updated.insert(close_brace,
                   ",\"executable_name\":\"" + name + "\",\"executable_b64\":\"" + b64 + "\"");
    return updated;
}

// add client_callback_address to the json payload
string attach_address(const string& payload, const string& callback_address) {
    if (callback_address.empty()) {
        return payload;
    }

    // position of the closing brace
    size_t close_brace = payload.rfind('}');
    if (close_brace == string::npos) {
        return payload;
    }

    // add the field before it
    string updated = payload;
    updated.insert(close_brace, ",\"client_callback_address\":\"" + callback_address + "\"");
    return updated;
}

// if the master fails over and later sends back a result, this listener receives it.
void recovery_listener(int callback_port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        cerr << "> Recovery listener socket creation failed." << endl;
        return;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(callback_port);

    // bind port
    if (::bind(server_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        cerr << "> Recovery listener bind failed on port " << callback_port << "." << endl;
        close(server_fd);
        return;
    }

    listen(server_fd, 10);

    // loop to get results
    while (true) {
        socklen_t addrlen = sizeof(address);
        // accept server_fd
        int client_socket = accept(server_fd, reinterpret_cast<sockaddr*>(&address), &addrlen);
        if (client_socket < 0) {
            continue;
        }

        string message = read_payload(client_socket);
        if (!message.empty()) {
            cout << "\n > Received result after master failover:" << endl;
            cout << message << endl;
            cout << "\n Enter config location (or exit to quit): " << flush;
        }
        close(client_socket);
    }
}

}

// read one length-prefixed message
string read_payload(int socket_fd) {
    uint64_t net_len = 0;
    unsigned char* len_bytes = reinterpret_cast<unsigned char*>(&net_len);
    size_t got = 0;
    while (got < sizeof(net_len)) {
        int n = read(socket_fd, len_bytes + got, sizeof(net_len) - got);
        if (n <= 0) return "";
        got += static_cast<size_t>(n);
    }

    uint64_t len = 0;
    // first 8 bytes = message length
    for (int i = 0; i < 8; ++i) {
        len = (len << 8) | static_cast<uint64_t>(len_bytes[i]);
    }

    // actual message
    string payload;
    payload.resize(static_cast<size_t>(len));
    size_t off = 0;
    while (off < payload.size()) {
        int n = read(socket_fd, &payload[off], payload.size() - off);
        if (n <= 0) return "";
        off += static_cast<size_t>(n);
    }
    return payload;
}

// send one length-prefixed message
bool sent_payload(int socket_fd, const string& payload) {
    uint64_t len = static_cast<uint64_t>(payload.size());
    // 8 byte header = message length
    unsigned char header[8];
    for (int i = 7; i >= 0; --i) {
        header[i] = static_cast<unsigned char>(len & 0xFF);
        len >>= 8;
    }
    if (send(socket_fd, header, sizeof(header), 0) != static_cast<int>(sizeof(header))) {
        return false;
    }

    // then the message
    size_t off = 0;
    while (off < payload.size()) {
        int n = send(socket_fd, payload.data() + off, payload.size() - off, 0);
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

#include "../common/config_parser.hpp"

int main() {
    cout << "Starting Client" << endl;
    // read client_config.json
    string sys_config_json = common::read_file("client_config.json");
    // read addresses and ports from the config
    string master_addr = common::extract_string(sys_config_json, "master_address", "");
    int master_port = common::extract_int(sys_config_json, "master_tcp_port", 8080);
    string configured_backup_addr = common::extract_string(sys_config_json, "backup_address", "");
    int configured_backup_port = common::extract_int(sys_config_json, "backup_tcp_port", 0);
    string callback_addr = common::extract_string(sys_config_json, "client_callback_address", "");

    // validation check
    if (master_addr.empty()) {
        cerr << "master_address in client_config.json is missing" << endl;
        return 1;
    }
    
    // semi-unique port for the callback listener
    int callback_port = create_callback_port();
    
    // the backup uses this listener to send results after a failover
    thread listener_thread(recovery_listener, callback_port);
    listener_thread.detach();
    string backup_addr = configured_backup_addr;
    int backup_port = configured_backup_port;

    // ask the primary where the backup is
    int disc_sock = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in disc_addr;
    disc_addr.sin_family = AF_INET;
    disc_addr.sin_port = htons(master_port);
    inet_pton(AF_INET, master_addr.c_str(), &disc_addr.sin_addr);

    if (connect(disc_sock, (struct sockaddr *)&disc_addr, sizeof(disc_addr)) >= 0) {
        string handshake = "{\"request\":\"discover_backup\"}";
        sent_payload(disc_sock, handshake);
        string response = read_payload(disc_sock);
        string discovered_backup_addr = common::extract_string(response, "backup_address");
        int discovered_backup_port = common::extract_int(response, "backup_tcp_port");
        if (!discovered_backup_addr.empty() && discovered_backup_port > 0) {
            backup_addr = discovered_backup_addr;
            backup_port = discovered_backup_port;
        }
        cout << "Backup is at " << backup_addr << ":" << backup_port << endl;
    } else {
        if (!backup_addr.empty() && backup_port > 0) {
            cout << "Could not reach the primary. Using the configured backup at "
                 << backup_addr << ":" << backup_port << endl;
        } else {
            cout << "Could not reach the primary, and no backup is configured." << endl;
        }
    }
    close(disc_sock);
    
    while (true) {
        cout << "\n > Enter config location (or exit to quit): ";
        string config_path;
        // stop on exit or quit
        if (!getline(cin, config_path) || config_path == "exit" || config_path == "quit") {
            break;
        }
        
        if (config_path.empty()) {
            continue;
        }


        string client_payload;
        try {

            client_payload = client_jobs::read_file(config_path);
        } catch (const std::exception& e) {
            cerr << " " << e.what() << endl;
            continue;
        }
        if (client_payload.empty()) {
            continue;
        }

        client_jobs::Job client_job("", "", 0, 0, 0, 0, 0, 0);
        try {
            client_job = client_jobs::get_job(config_path);
        } catch (const std::exception& e) {
            cerr << " Invalid job config " << config_path << ": " << e.what() << endl;

            continue;
        }
        // add the callback port, callback address and executable to the payload
        client_payload = attach_port(client_payload, callback_port);
        client_payload = attach_address(client_payload, callback_addr);
        client_payload = attach_upload(client_payload, client_job.executable);        
        int sock = 0;

        struct sockaddr_in serv_addr;
        if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            cerr << "Socket creation error" << endl;
            continue;
        }
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(master_port);
        if (inet_pton(AF_INET, master_addr.c_str(), &serv_addr.sin_addr) <= 0) {
            cerr << " Invalid address or Address not supported" << endl;
            close(sock);
            continue;
        }
        

        // connect to the primary
        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            cerr << "> Connection to the primary failed (" << master_addr << ":" << master_port << ")" << endl;
            close(sock);
            // fall back to the backup
            if (!backup_addr.empty() && backup_port > 0) {
                cout << "> Trying the backup at " << backup_addr << ":" << backup_port << "..." << endl;
                sock = socket(AF_INET, SOCK_STREAM, 0);
                serv_addr.sin_port = htons(backup_port);
                inet_pton(AF_INET, backup_addr.c_str(), &serv_addr.sin_addr);
                if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
                    cerr << "> Connection to the backup also failed." << endl;
                    close(sock);
                    continue;
                }
            } else {
                continue;
            }
        }
        
        cout << "> Connected. Sending job." << endl;
        sent_payload(sock, client_payload);
        
        cout << "> Waiting for response..." << endl;
        string response = read_payload(sock);

        if (!response.empty()) {
            cout << " Received response:" << endl;
            cout << response << endl;
        } else {
            cout << " Connection closed without a response. If the backup takes over, the result will arrive on the callback listener." << endl;
        }
        
        close(sock);
    }
    
    return 0;
}
