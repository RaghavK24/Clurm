#include "jobs/job.hpp"
#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <cstdint>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sqlite3.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <atomic>
#include <array>
#include <vector>
#include <algorithm>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <sys/wait.h>
#include <signal.h>
#include <regex>
#include <sstream>
#include <sys/stat.h>

#include "../common/base64.hpp"

using namespace std;
using namespace std::chrono;

// free resources, advertised in every heartbeat
mutex db_mutex;
atomic<int> available_resources(100);
atomic<int> heartbeat_interval_ms(200);
atomic<int> free_cores(1);
atomic<int> free_memory_mb(512);
atomic<int> free_gpus(0);
string global_master_addr = "";
int global_master_udp_port = 0;
string global_backup_addr = "";
int global_backup_udp_port = 0;

// current time in UTC
string current_timestamp_utc() {
    const auto now = system_clock::now();
    const time_t now_time = system_clock::to_time_t(now);
    tm utc_time{};
    gmtime_r(&now_time, &utc_time);

    ostringstream out;
    out << put_time(&utc_time, "%Y-%m-%dT%H:%M:%SZ");
    return out.str();
}

// initialize database
void init_db() {
    sqlite3* db;
    if (sqlite3_open("compute_logs.db", &db) != SQLITE_OK) return;
    string sql = "CREATE TABLE IF NOT EXISTS execution_log ("
                 "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                 "submission_id INTEGER NOT NULL DEFAULT -1, "
                 "job_name TEXT NOT NULL, "
                 "executable TEXT NOT NULL, "
                 "execution_state TEXT NOT NULL DEFAULT 'COMPLETED', "
                 "return_code INTEGER NOT NULL, "
                 "completion_status TEXT NOT NULL, "
                 "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);";
    sqlite3_exec(db, sql.c_str(), 0, 0, 0);
    vector<string> migrations = {
        "ALTER TABLE execution_log ADD COLUMN submission_id INTEGER NOT NULL DEFAULT -1;",
        "ALTER TABLE execution_log ADD COLUMN execution_state TEXT NOT NULL DEFAULT 'COMPLETED';"
    };
    for (const string& migration : migrations) {
        sqlite3_exec(db, migration.c_str(), 0, 0, 0);
    }
    sqlite3_close(db);
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
    // then the message
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
bool send_payload(int socket_fd, const string& payload) {
    uint64_t len = static_cast<uint64_t>(payload.size());
    unsigned char header[8];
    // 8 byte header = message length
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

// escape single quotes for sql
string formatsql(const string& value) {
    string escaped;
    escaped.reserve(value.size());
    for (char ch : value) {
        if (ch == '\'') {
            escaped += "''";
        } else {
            escaped += ch;
        }
    }
    return escaped;
}

// insert or update the execution_log row for a submission
void update_logs(int submission_id,
                          const string& job_name,
                          const string& executable,
                          const string& execution_state,
                          int return_code,
                          const string& completion_status) {
    lock_guard<mutex> lock(db_mutex);
    sqlite3* db;
    if (sqlite3_open("compute_logs.db", &db) != SQLITE_OK) return;
    string escaped_job_name = formatsql(job_name);
    string escaped_executable = formatsql(executable);
    string escaped_state = formatsql(execution_state);
    string escaped_status = formatsql(completion_status);
    string sql = "INSERT OR REPLACE INTO execution_log "
                 "(id, submission_id, job_name, executable, execution_state, return_code, completion_status, timestamp) "
                 "VALUES ("
                 "(SELECT id FROM execution_log WHERE submission_id = " + to_string(submission_id) + "), " +
                 to_string(submission_id) + ", '" + escaped_job_name + "', '" + escaped_executable + "', '" +
                 escaped_state + "', " + to_string(return_code) + ", '" + escaped_status + "', CURRENT_TIMESTAMP);";
    sqlite3_exec(db, sql.c_str(), 0, 0, 0);
    sqlite3_close(db);
}

// run the executable and capture its output
string execute(const string& executable, int& return_code) {
    // capture stderr together with stdout
    string command = "\"" + executable + "\" 2>&1";
    array<char, 256> buffer{};
    string output;

    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) {
        return_code = -1;
        return "Failed to start executable";
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        output += buffer.data();
    }

    int status = pclose(pipe);
    if (status == -1) {
        return_code = -1;
    } else if (WIFEXITED(status)) {
        return_code = WEXITSTATUS(status);
    } else {
        return_code = status;
    }

    if (output.empty()) {
        output = "(no output)";
    }
    return output;
}

// send heartbeats over UDP to the primary and the backup
void send_heartbeats(string port, string master_addr, int master_udp_port, string backup_addr, int backup_udp_port) {
    int sockfd;
    struct sockaddr_in servaddr, backup_servaddr;
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) return;
    
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(master_udp_port);
    inet_pton(AF_INET, master_addr.c_str(), &servaddr.sin_addr);

    bool has_backup = !backup_addr.empty() && backup_udp_port > 0;
    if (has_backup) {
        memset(&backup_servaddr, 0, sizeof(backup_servaddr));
        backup_servaddr.sin_family = AF_INET;
        backup_servaddr.sin_port = htons(backup_udp_port);
        inet_pton(AF_INET, backup_addr.c_str(), &backup_servaddr.sin_addr);
    }
    
    while (true) {
        // port:capacity:cores:memory:gpus
        string payload = port + ":" +
                          to_string(available_resources.load()) + ":" +
                          to_string(free_cores.load()) + ":" +
                          to_string(free_memory_mb.load()) + ":" +
                          to_string(free_gpus.load());
        sendto(sockfd, payload.c_str(), payload.length(), 0, (const struct sockaddr *) &servaddr, sizeof(servaddr));
        if (has_backup) {
            sendto(sockfd, payload.c_str(), payload.length(), 0, (const struct sockaddr *) &backup_servaddr, sizeof(backup_servaddr));
        }
        this_thread::sleep_for(milliseconds(heartbeat_interval_ms.load()));
    }
}

// send a result over UDP to the primary and, if configured, the backup
void send_result(const string& message,
                     const string& master_addr,
                     int master_udp_port,
                     const string& backup_addr,
                     int backup_udp_port) {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) return;

    sockaddr_in servaddr{};
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(master_udp_port);
    if (inet_pton(AF_INET, master_addr.c_str(), &servaddr.sin_addr) > 0) {
        sendto(sockfd,
               message.c_str(),
               message.length(),
               0,
               reinterpret_cast<const struct sockaddr*>(&servaddr),
               sizeof(servaddr));
    }

    if (!backup_addr.empty() && backup_udp_port > 0) {
        sockaddr_in backupaddr{};
        backupaddr.sin_family = AF_INET;
        backupaddr.sin_port = htons(backup_udp_port);
        if (inet_pton(AF_INET, backup_addr.c_str(), &backupaddr.sin_addr) > 0) {
            sendto(sockfd,
                   message.c_str(),
                   message.length(),
                   0,
                   reinterpret_cast<const struct sockaddr*>(&backupaddr),
                   sizeof(backupaddr));
        }
    }

    close(sockfd);
}

// run one job forwarded by the master
void handle_master(int master_socket) {
    string master_payload = read_payload(master_socket);
    if (!master_payload.empty()) {
        // reserve resources until the job finishes
        available_resources -= 20;
        int reserved_cores = 0;
        int reserved_memory_mb = 0;
        int reserved_gpus = 0;
        
        try {
            compute_jobs::Job compute_job = compute_jobs::receive_forwarded_job(master_payload);
            reserved_cores = max(0, compute_job.min_cores);
            reserved_memory_mb = max(0, compute_job.min_memory);
            reserved_gpus = max(0, compute_job.gpu_required);
            free_cores -= reserved_cores;
            free_memory_mb -= reserved_memory_mb;
            free_gpus -= reserved_gpus;
            cout << "\n > Executing job " << compute_job.name  << endl;
            string exec_path = compute_job.executable;
            string tmp_path;
            // the client uploads the executable as base64; write it to a temp file
            if (!compute_job.executable_b64.empty()) {
                vector<unsigned char> bytes = common::base64_decode(compute_job.executable_b64);
                string tmpl = "/tmp/cluster_job_" + to_string(compute_job.submission_id) + "_XXXXXX";
                vector<char> buf(tmpl.begin(), tmpl.end());
                buf.push_back('\0');
                int fd = mkstemp(buf.data());
                if (fd < 0) {
                    throw runtime_error("Unable to create temp executable");
                }
                ssize_t wrote = write(fd, bytes.data(), bytes.size());
                close(fd);
                if (wrote < 0 || static_cast<size_t>(wrote) != bytes.size()) {
                    unlink(buf.data());
                    throw runtime_error("Unable to write temp executable");
                }
                chmod(buf.data(), 0755);
                tmp_path = string(buf.data());
                exec_path = tmp_path;
            }

            compute_job.start_execution(current_timestamp_utc());
            update_logs(compute_job.submission_id,
                                 compute_job.name,
                                 exec_path,
                                 "RUNNING",
                                 -1,
                                 "RUNNING");
            this_thread::sleep_for(seconds(2));
            
            int result = 0;
            string job_output = execute(exec_path, result);
            
            compute_job.finish_execution(10,current_timestamp_utc());
            string response = "Execution completed " + to_string(result) + "\nOUTPUT:\n" + job_output;
            update_logs(compute_job.submission_id,
                                 compute_job.name,
                                 exec_path,
                                 "COMPLETED",
                                 result,
                                 response);
            // reply to the master over TCP
            send_payload(master_socket, response);
            // and over UDP, so the backup also gets the result
            if (!global_master_addr.empty() && global_master_udp_port > 0) {
                string udp_msg = "RESULT:" + to_string(compute_job.submission_id) + ":" + response;
                send_result(udp_msg,
                                global_master_addr,
                                global_master_udp_port,
                                global_backup_addr,
                                global_backup_udp_port);
            }
            
            if (!tmp_path.empty()) {
                unlink(tmp_path.c_str());
            }

            // release the reserved resources
            available_resources += 20;
            free_cores += reserved_cores;
            free_memory_mb += reserved_memory_mb;
            free_gpus += reserved_gpus;
        } catch (const exception& e) {
            string err_msg = string("COMPUTE ERROR: ") + e.what();
            send_payload(master_socket, err_msg);

            int submission_id = -1;
            {
                const regex pattern("\\\"submission_id\\\"\\s*:\\s*(-?\\d+)");
                smatch match;
                if (regex_search(master_payload, match, pattern)) {
                    submission_id = stoi(match[1].str());
                }
            }
            if (!global_master_addr.empty() && global_master_udp_port > 0 && submission_id > 0) {
                string udp_msg = "RESULT:" + to_string(submission_id) + ":" + err_msg;
                send_result(udp_msg,
                                global_master_addr,
                                global_master_udp_port,
                                global_backup_addr,
                                global_backup_udp_port);
            }

            // release the reserved resources
            available_resources += 20;
            free_cores += reserved_cores;
            free_memory_mb += reserved_memory_mb;
            free_gpus += reserved_gpus;
        }
    }
    close(master_socket);
}

#include "../common/config_parser.hpp"

int main(int argc, char const *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    string config_json = common::read_file("compute_config.json");
    string compute_addr = common::extract_string(config_json, "compute_address", "0.0.0.0");
    int port = common::extract_int(config_json, "compute_tcp_port", 9090);
    string master_addr = common::extract_string(config_json, "master_address", "");
    int master_udp_port = common::extract_int(config_json, "master_udp_port", 8081);
    string backup_addr = common::extract_string(config_json, "backup_address", "");
    int backup_udp_port = common::extract_int(config_json, "backup_udp_port", 0);

    if (master_addr.empty()) {
        cerr << "> Missing required config: master_address in compute_config.json" << endl;
        return 1;
    }

    global_master_addr = master_addr;
    global_master_udp_port = master_udp_port;
    global_backup_addr = backup_addr;
    global_backup_udp_port = backup_udp_port;

    // resources this node advertises
    available_resources.store(common::extract_int(config_json, "compute_available_resources", 100));
    heartbeat_interval_ms.store(common::extract_int(config_json, "heartbeat_interval_ms", 200));
    free_cores.store(common::extract_int(config_json, "compute_free_cores", 1));
    free_memory_mb.store(common::extract_int(config_json, "compute_free_memory_mb", 512));
    free_gpus.store(common::extract_int(config_json, "compute_free_gpus", 0));

    if (argc > 1) {
        port = stoi(argv[1]);
    }
    init_db();
    
    thread heartbeat(send_heartbeats, to_string(port), master_addr, master_udp_port, backup_addr, backup_udp_port);
    heartbeat.detach();

    int server_fd, new_socket;
    struct sockaddr_in address{};
    int opt = 1;
    int addrlen = sizeof(address);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));
    address.sin_family = AF_INET;
    inet_pton(AF_INET, compute_addr.c_str(), &address.sin_addr);
    address.sin_port = htons(port);

    if (::bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        cerr << "> TCP port " << port << " is not available on " << compute_addr << ". Exiting." << endl;
        close(server_fd);
        return 1;
    }
    listen(server_fd, 10);
    
    cout << "> Listening on port " << port << ", heartbeat every " << heartbeat_interval_ms.load() << " ms" << endl;
    while (true) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) >= 0) {
            thread t(handle_master, new_socket);
            t.detach();
        }
    }
    return 0;
}
