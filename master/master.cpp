#include "jobs/job.hpp"
#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <cerrno>
#include <fcntl.h>
#include <sqlite3.h>
#include <algorithm>
#include <cstdint>
#include <thread>
#include <mutex>
#include <chrono>
#include <unordered_map>
#include <atomic>
#include <signal.h>
#include <regex>
#include <algorithm>
#include <vector>

using namespace std;
using namespace std::chrono;

string read_socket_payload(int socket_fd);
bool send_socket_payload(int socket_fd, const string& payload);

struct ComputeHealth {
    steady_clock::time_point last_ping;
    string address;
    int port;
    int max_resources;
    int free_cores;
    int free_memory_mb;
    int free_gpus;
};

struct ComputeTarget {
    string key;
    string address;
    int port;
    int reserved_resources = 0;
    int reserved_cores = 0;
    int reserved_memory_mb = 0;
    int reserved_gpus = 0;
    bool valid = false;
};

const int kJobResourceCost = 20;

mutex db_mutex;
mutex nodes_mutex;
unordered_map<string, ComputeHealth> compute_nodes;
atomic<int> next_submission_id(1001);
atomic<int> next_receipt_id(2001);
int global_heartbeat_timeout_ms = 2000;
int global_monitor_interval_ms = 200;

string global_backup_address = "";
int global_backup_tcp_port = 0;
int global_backup_connect_timeout_ms = 200;

namespace {

const char* kDbPath = "master_logs.db";

string json_escape(const string& value) {
    string out;
    out.reserve(value.size());
    for (char ch : value) {
        switch (ch) {
            case '\\': out += "\\\\"; break;
            case '"': out += "\\\""; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default: out += ch; break;
        }
    }
    return out;
}

string escape_sql(const string& value) {
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

int extract_optional_int(const string& payload, const string& key, int fallback = 0) {
    string search = "\"" + key + "\"";
    size_t pos = payload.find(search);
    if (pos == string::npos) return fallback;
    pos = payload.find(':', pos + search.length());
    if (pos == string::npos) return fallback;
    pos++;
    while (pos < payload.length() && isspace(payload[pos])) pos++;
    size_t end = pos;
    if (end < payload.length() && payload[end] == '-') end++;
    while (end < payload.length() && isdigit(payload[end])) end++;
    if (pos == end) return fallback;
    return stoi(payload.substr(pos, end - pos));
}

bool is_transport_failure(const string& response) {
    return response == "Socket creation error" ||
           response == "Connection to Compute Node Failed" ||
           response == "No response from Compute node" ||
           response == "Compute send failed";
}

string extract_optional_string(const string& payload, const string& key, const string& fallback = "") {
    string search = "\"" + key + "\"";
    size_t pos = payload.find(search);
    if (pos == string::npos) return fallback;
    pos = payload.find(':', pos + search.length());
    if (pos == string::npos) return fallback;
    pos = payload.find('"', pos);
    if (pos == string::npos) return fallback;
    pos++;
    size_t end = pos;
    while (end < payload.length() && payload[end] != '"') {
        if (payload[end] == '\\') end += 2;
        else end++;
    }
    if (end >= payload.length()) return fallback;
    return payload.substr(pos, end - pos);
}

string socket_ip(int socket_fd) {
    sockaddr_in addr{};
    socklen_t addr_len = sizeof(addr);
    if (getpeername(socket_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len) != 0) {
        return "";
    }

    char ip_buffer[INET_ADDRSTRLEN] = {};
    if (inet_ntop(AF_INET, &addr.sin_addr, ip_buffer, sizeof(ip_buffer)) == nullptr) {
        return "";
    }
    return string(ip_buffer);
}

bool deliver_to_client(const string& client_callback_address, int client_callback_port, const string& message) {
    if (client_callback_port <= 0 || client_callback_address.empty()) {
        return false;
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        return false;
    }

    sockaddr_in client_addr{};
    client_addr.sin_family = AF_INET;
    client_addr.sin_port = htons(client_callback_port);
    if (inet_pton(AF_INET, client_callback_address.c_str(), &client_addr.sin_addr) <= 0) {
        close(sock);
        return false;
    }

    if (connect(sock, reinterpret_cast<sockaddr*>(&client_addr), sizeof(client_addr)) < 0) {
        close(sock);
        return false;
    }

    send_socket_payload(sock, message);
    close(sock);
    return true;
}

bool replicate_to_backup(const string& payload) {
    if (global_backup_address.empty() || global_backup_tcp_port <= 0) {
        return false;
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(global_backup_tcp_port);
    if (inet_pton(AF_INET, global_backup_address.c_str(), &addr.sin_addr) <= 0) {
        close(sock);
        return false;
    }

    int flags = fcntl(sock, F_GETFL, 0);
    if (flags < 0 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(sock);
        return false;
    }

    int rc = connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        close(sock);
        return false;
    }

    if (rc < 0) {
        fd_set write_set;
        FD_ZERO(&write_set);
        FD_SET(sock, &write_set);

        timeval timeout{};
        timeout.tv_sec = global_backup_connect_timeout_ms / 1000;
        timeout.tv_usec = (global_backup_connect_timeout_ms % 1000) * 1000;

        rc = select(sock + 1, nullptr, &write_set, nullptr, &timeout);
        if (rc <= 0) {
            close(sock);
            return false;
        }

        int socket_error = 0;
        socklen_t len = sizeof(socket_error);
        if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &socket_error, &len) < 0 || socket_error != 0) {
            close(sock);
            return false;
        }
    }

    fcntl(sock, F_SETFL, flags);

    bool ok = send_socket_payload(sock, payload);
    close(sock);
    return ok;
}

void init_sequence_counters() {
    sqlite3* db;
    if (sqlite3_open(kDbPath, &db) != SQLITE_OK) {
        return;
    }

    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db,
                           "SELECT COALESCE(MAX(submission_id), 1000), COALESCE(MAX(id), 2000) FROM jobs_log;",
                           -1,
                           &stmt,
                           nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            next_submission_id.store(sqlite3_column_int(stmt, 0) + 1);
            next_receipt_id.store(sqlite3_column_int(stmt, 1) + 1);
        }
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

void insert_job_log(int submission_id,
                    const string& job_name,
                    const string& sender,
                    const string& forwarding_status,
                    const string& response_status,
                    const string& client_callback_address,
                    int client_callback_port,
                    const string& forward_payload) {
    lock_guard<mutex> lock(db_mutex);
    sqlite3* db;
    if (sqlite3_open(kDbPath, &db) != SQLITE_OK) return;
    string sql = "INSERT INTO jobs_log "
                 "(submission_id, job_name, sender, forwarding_status, response_status, client_callback_address, client_callback_port, forward_payload, delivery_status) "
                 "VALUES (" + to_string(submission_id) + ", '" + escape_sql(job_name) + "', '" + escape_sql(sender) +
                 "', '" + escape_sql(forwarding_status) + "', '" + escape_sql(response_status) + "', '" +
                 escape_sql(client_callback_address) + "', " +
                 to_string(client_callback_port) + ", '" + escape_sql(forward_payload) + "', 'PENDING');";
    sqlite3_exec(db, sql.c_str(), 0, 0, 0);
    sqlite3_close(db);

    string repl = "{\"request\":\"replicate\",\"action\":\"insert\",\"submission_id\":" + to_string(submission_id) +
                  ",\"job_name\":\"" + json_escape(job_name) + "\",\"sender\":\"" + json_escape(sender) +
                  "\",\"forwarding_status\":\"" + json_escape(forwarding_status) + "\",\"response_status\":\"" +
                  json_escape(response_status) + "\",\"client_callback_address\":\"" + json_escape(client_callback_address) +
                  "\",\"client_callback_port\":" + to_string(client_callback_port) + ",\"forward_payload\":\"" +
                  json_escape(forward_payload) + "\"}";
    replicate_to_backup(repl);
}

void update_job_log(int submission_id,
                    const string& forwarding_status,
                    const string& response_status,
                    const string& delivery_status) {
    lock_guard<mutex> lock(db_mutex);
    sqlite3* db;
    if (sqlite3_open(kDbPath, &db) != SQLITE_OK) return;
    string sql = "UPDATE jobs_log SET "
                 "forwarding_status = '" + escape_sql(forwarding_status) + "', "
                 "response_status = '" + escape_sql(response_status) + "', "
                 "delivery_status = '" + escape_sql(delivery_status) + "', "
                 "timestamp = CURRENT_TIMESTAMP "
                 "WHERE submission_id = " + to_string(submission_id) + ";";
    sqlite3_exec(db, sql.c_str(), 0, 0, 0);
    sqlite3_close(db);

    string repl = "{\"request\":\"replicate\",\"action\":\"update\",\"submission_id\":" + to_string(submission_id) +
                  ",\"forwarding_status\":\"" + json_escape(forwarding_status) + "\",\"response_status\":\"" +
                  json_escape(response_status) + "\",\"delivery_status\":\"" + json_escape(delivery_status) + "\"}";
    replicate_to_backup(repl);
}

// a compute node's UDP result can arrive after the job was already answered over TCP,
// so only store it for jobs that are still waiting. the backup gets the same UDP message.
void store_compute_result(int submission_id, const string& response) {
    lock_guard<mutex> lock(db_mutex);
    sqlite3* db;
    if (sqlite3_open(kDbPath, &db) != SQLITE_OK) return;
    string sql = "UPDATE jobs_log SET "
                 "forwarding_status = 'FORWARDED', "
                 "response_status = '" + escape_sql(response) + "', "
                 "timestamp = CURRENT_TIMESTAMP "
                 "WHERE submission_id = " + to_string(submission_id) + " AND delivery_status = 'PENDING';";
    sqlite3_exec(db, sql.c_str(), 0, 0, 0);
    sqlite3_close(db);
}

}

string read_socket_payload(int socket_fd) {
    uint64_t net_len = 0;
    unsigned char* len_bytes = reinterpret_cast<unsigned char*>(&net_len);
    size_t got = 0;
    while (got < sizeof(net_len)) {
        int n = read(socket_fd, len_bytes + got, sizeof(net_len) - got);
        if (n <= 0) return "";
        got += static_cast<size_t>(n);
    }

    uint64_t len = 0;
    for (int i = 0; i < 8; ++i) {
        len = (len << 8) | static_cast<uint64_t>(len_bytes[i]);
    }

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

bool send_socket_payload(int socket_fd, const string& payload) {
    uint64_t len = static_cast<uint64_t>(payload.size());
    unsigned char header[8];
    for (int i = 7; i >= 0; --i) {
        header[i] = static_cast<unsigned char>(len & 0xFF);
        len >>= 8;
    }
    if (send(socket_fd, header, sizeof(header), 0) != static_cast<int>(sizeof(header))) {
        return false;
    }

    size_t off = 0;
    while (off < payload.size()) {
        int n = send(socket_fd, payload.data() + off, payload.size() - off, 0);
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

void init_db() {
    sqlite3* db;
    if (sqlite3_open(kDbPath, &db) != SQLITE_OK) return;
    string sql = "CREATE TABLE IF NOT EXISTS jobs_log ("
                 "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                 "submission_id INTEGER NOT NULL, "
                 "job_name TEXT NOT NULL, "
                 "sender TEXT NOT NULL, "
                 "forwarding_status TEXT NOT NULL, "
                 "response_status TEXT NOT NULL, "
                 "client_callback_address TEXT NOT NULL DEFAULT '', "
                 "client_callback_port INTEGER NOT NULL DEFAULT 0, "
                 "forward_payload TEXT NOT NULL DEFAULT '', "
                 "delivery_status TEXT NOT NULL DEFAULT 'PENDING', "
                 "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);";
    sqlite3_exec(db, sql.c_str(), 0, 0, 0);
    vector<string> migrations = {
        "ALTER TABLE jobs_log ADD COLUMN client_callback_address TEXT NOT NULL DEFAULT '';",
        "ALTER TABLE jobs_log ADD COLUMN client_callback_port INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE jobs_log ADD COLUMN forward_payload TEXT NOT NULL DEFAULT '';",
        "ALTER TABLE jobs_log ADD COLUMN delivery_status TEXT NOT NULL DEFAULT 'PENDING';"
    };
    for (const string& migration : migrations) {
        sqlite3_exec(db, migration.c_str(), 0, 0, 0);
    }
    sqlite3_close(db);
    init_sequence_counters();
}

void log_master(int submission_id, const string& job_name, const string& sender, const string& fw_status, const string& rsp_status) {
    update_job_log(submission_id, fw_status, rsp_status, "DIRECT");
}

string forward_to_compute(const string& master_payload, const ComputeTarget& compute_target) {
    int sock = 0;
    struct sockaddr_in serv_addr;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) return "Socket creation error";
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(compute_target.port);
    if (inet_pton(AF_INET, compute_target.address.c_str(), &serv_addr.sin_addr) <= 0) {
        close(sock);
        return "Connection to Compute Node Failed";
    }
    
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sock);
        return "Connection to Compute Node Failed";
    }
    if (!send_socket_payload(sock, master_payload)) {
        close(sock);
        return "Compute send failed";
    }
    
    string response = read_socket_payload(sock);
    close(sock);
    if (!response.empty()) return response;
    return "No response from Compute node";
}

ComputeTarget reserve_compute_node(const master_jobs::Job& job) {
    lock_guard<mutex> lock(nodes_mutex);
    ComputeTarget target;
    int most_cap = -1;

    for (auto& pair : compute_nodes) {
        const ComputeHealth& node = pair.second;
        if (node.max_resources < kJobResourceCost) {
            continue;
        }
        if (node.free_cores >= 0 && node.free_cores < job.min_cores) {
            continue;
        }
        if (node.free_memory_mb >= 0 && node.free_memory_mb < job.min_memory) {
            continue;
        }
        if (node.free_gpus >= 0 && node.free_gpus < job.gpu_required) {
            continue;
        }

        if (node.max_resources > most_cap) {
            most_cap = pair.second.max_resources;
            target = {pair.first, pair.second.address, pair.second.port, kJobResourceCost, job.min_cores, job.min_memory, job.gpu_required, true};
        }
    }

    if (target.valid) {
        ComputeHealth& selected = compute_nodes[target.key];
        selected.max_resources = max(0, selected.max_resources - target.reserved_resources);
        if (selected.free_cores >= 0) selected.free_cores = max(0, selected.free_cores - target.reserved_cores);
        if (selected.free_memory_mb >= 0) selected.free_memory_mb = max(0, selected.free_memory_mb - target.reserved_memory_mb);
        if (selected.free_gpus >= 0) selected.free_gpus = max(0, selected.free_gpus - target.reserved_gpus);
    }

    return target;
}

void restore_reserved_resources(const ComputeTarget& target) {
    lock_guard<mutex> lock(nodes_mutex);
    auto it = compute_nodes.find(target.key);
    if (it != compute_nodes.end()) {
        it->second.max_resources += target.reserved_resources;
        if (it->second.free_cores >= 0) it->second.free_cores += target.reserved_cores;
        if (it->second.free_memory_mb >= 0) it->second.free_memory_mb += target.reserved_memory_mb;
        if (it->second.free_gpus >= 0) it->second.free_gpus += target.reserved_gpus;
    }
}

void handle_client(int client_socket) {
    string client_payload = read_socket_payload(client_socket);
    if (!client_payload.empty()) {
        if (client_payload.find("\"request\":\"discover_backup\"") != string::npos) {
            string response = "{\"backup_address\":\"" + global_backup_address + "\", \"backup_tcp_port\":" + to_string(global_backup_tcp_port) + "}";
            send_socket_payload(client_socket, response);
            close(client_socket);
            return;
        }
        
        try {
            int submission_id = next_submission_id.fetch_add(1);
            int receipt_id = next_receipt_id.fetch_add(1);
            int callback_port = extract_optional_int(client_payload, "client_callback_port", 0);
            string callback_address = extract_optional_string(client_payload, "client_callback_address", socket_ip(client_socket));
            master_jobs::Job mj = master_jobs::receive_job_from_client(client_payload, submission_id, receipt_id, "client-tcp");
            string fp = mj.to_forward_payload();
            insert_job_log(mj.submission_id, mj.name, mj.sender, "PENDING", "PENDING", callback_address, callback_port, fp);
            
            string response;
            bool success = false;
            int max_retries = 3;
            
            for (int attempt = 0; attempt < max_retries && !success; ++attempt) {
                ComputeTarget target = reserve_compute_node(mj);
                
                if (!target.valid) {
                   response = "MASTER ERROR: no compute node available";
                   this_thread::sleep_for(milliseconds(500));
                   continue;
                }
                
                cout << "\n[PRIMARY] Sending submission " << mj.submission_id << " to "
                     << target.address << ":" << target.port << endl;
                response = forward_to_compute(fp, target);
                
                if (!is_transport_failure(response)) {
                    success = true;
                } else {
                    {
                        lock_guard<mutex> lock(nodes_mutex);
                        compute_nodes.erase(target.key);
                    }
                    cout << "[PRIMARY WARN] Compute node " << target.address << ":" << target.port
                         << " did not respond. Removed it, retrying." << endl;
                }
            }
            
            if (!success) {
                response = "MASTER ERROR: job failed on every attempt";
            }

            log_master(mj.submission_id, mj.name, mj.sender, success ? "FORWARDED" : "FAILED", response);
            string final_response = "MASTER RESPONSE: " + response;
            send_socket_payload(client_socket, final_response);
        } catch (const exception& e) {
            string err_msg = string("MASTER ERROR: could not parse job: ") + e.what();
            send_socket_payload(client_socket, err_msg);
        }
    }
    close(client_socket);
}

void udp_heartbeat_listener(int udp_sock) {
    char buffer[1024];
    struct sockaddr_in cliaddr;
    socklen_t len = sizeof(cliaddr);
    
    while (true) {
        int n = recvfrom(udp_sock, (char *)buffer, sizeof(buffer) - 1, MSG_WAITALL, (struct sockaddr *) &cliaddr, &len);
         if (n > 0) {
             buffer[n] = '\0';
             string ping_msg(buffer);

             if (ping_msg.rfind("RESULT:", 0) == 0) {
                 const regex pattern("^RESULT:(\\d+):(.*)$");
                 smatch match;
                 if (regex_search(ping_msg, match, pattern) && match.size() >= 3) {
                     int submission_id = stoi(match[1].str());
                     string response = match[2].str();
                     store_compute_result(submission_id, response);
                 }
                 continue;
             }

             vector<string> parts;
             {
                 size_t start = 0;
                 while (true) {
                     size_t pos = ping_msg.find(':', start);
                     if (pos == string::npos) {
                         parts.push_back(ping_msg.substr(start));
                         break;
                     }
                     parts.push_back(ping_msg.substr(start, pos - start));
                     start = pos + 1;
                 }
             }

             if (parts.size() >= 2) {
                 string port_str = parts[0];
                 int capacities = stoi(parts[1]);
                 int cores = -1;
                 int mem_mb = -1;
                 int gpus = -1;
                 if (parts.size() >= 5) {
                     cores = stoi(parts[2]);
                     mem_mb = stoi(parts[3]);
                     gpus = stoi(parts[4]);
                 } else {
                     // old heartbeat format, port:capacity only. cores and memory unknown, no gpus.
                     gpus = 0;
                 }
                 char ip_buffer[INET_ADDRSTRLEN] = {};
                 inet_ntop(AF_INET, &cliaddr.sin_addr, ip_buffer, sizeof(ip_buffer));
                 string node_ip(ip_buffer);
                 string node_key = node_ip + ":" + port_str;
                 
                 lock_guard<mutex> lock(nodes_mutex);
                 compute_nodes[node_key] = {steady_clock::now(), node_ip, stoi(port_str), capacities, cores, mem_mb, gpus};
             }
         }
     }
 }

void dead_compute_monitor() {
    while (true) {
        this_thread::sleep_for(milliseconds(global_monitor_interval_ms));
        auto now = steady_clock::now();
        lock_guard<mutex> lock(nodes_mutex);
        for (auto it = compute_nodes.begin(); it != compute_nodes.end();) {
            if (duration_cast<milliseconds>(now - it->second.last_ping).count() > global_heartbeat_timeout_ms) {
                cout << "[PRIMARY] Compute node " << it->first << " missed heartbeats. Removing it." << endl;
                it = compute_nodes.erase(it);
            } else {
                ++it;
            }
        }
    }
}

#include "../common/config_parser.hpp"

int main() {
    signal(SIGPIPE, SIG_IGN);
    init_db();
    int server_fd, udp_sock;
    struct sockaddr_in address{}, udp_addr{};
    int opt = 1;

    string config_json = common::read_file("master_config.json");
    string tcp_addr = common::extract_string(config_json, "tcp_address", "0.0.0.0");
    int tcp_port = common::extract_int(config_json, "tcp_port", 8080);
    string udp_addr_str = common::extract_string(config_json, "udp_address", "0.0.0.0");
    int udp_port = common::extract_int(config_json, "udp_port", 8081);
    
    global_heartbeat_timeout_ms = common::extract_int(config_json, "heartbeat_timeout_ms", 2000);
    global_monitor_interval_ms = common::extract_int(config_json, "monitor_interval_ms", 200);
    
    global_backup_address = common::extract_string(config_json, "backup_address", "");
    global_backup_tcp_port = common::extract_int(config_json, "backup_tcp_port", 8080);
    global_backup_connect_timeout_ms = common::extract_int(config_json, "backup_connect_timeout_ms", 200);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)); 
    address.sin_family = AF_INET;
    inet_pton(AF_INET, tcp_addr.c_str(), &address.sin_addr);
    address.sin_port = htons(tcp_port);

    udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    setsockopt(udp_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    udp_addr.sin_family = AF_INET;
    inet_pton(AF_INET, udp_addr_str.c_str(), &udp_addr.sin_addr);
    udp_addr.sin_port = htons(udp_port);

    cout << "[PRIMARY] Starting" << endl;
    if (::bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        cerr << "[PRIMARY ERROR] TCP port " << tcp_port << " is not available on " << tcp_addr << ". Exiting." << endl;
        close(server_fd);
        close(udp_sock);
        exit(EXIT_FAILURE);
    }
    
    if (::bind(udp_sock, (struct sockaddr *)&udp_addr, sizeof(udp_addr)) < 0) {
        cerr << "[PRIMARY ERROR] UDP port " << udp_port << " is not available on " << udp_addr_str << ". Exiting." << endl;
        close(server_fd);
        close(udp_sock);
        exit(EXIT_FAILURE);
    }

    cout << "[PRIMARY] Listening on " << tcp_addr << ":" << tcp_port << " (TCP) and " << udp_addr_str << ":" << udp_port << " (UDP)" << endl;

    listen(server_fd, 10);
    
    thread udp_thread(udp_heartbeat_listener, udp_sock);
    udp_thread.detach();
    thread monitor_thread(dead_compute_monitor);
    monitor_thread.detach();

    int addrlen = sizeof(address);
    while (true) {
        int new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
        if (new_socket >= 0) {
            thread t(handle_client, new_socket);
            t.detach();
        }
    }
    return 0;
}
