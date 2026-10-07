#pragma once

#include <cstddef>
#include <string>
#include <vector>

using namespace std;

namespace client_jobs {

// a job as read from the job config
class Job {
public:
    Job(const string& name,
        const string& executable,
        int priority,
        int time_required,
        int min_memory,
        int min_cores,
        int max_memory,
        int gpu_required);
    string get_executable_name() const;
    string to_string() const;
    bool operator==(const Job& other) const;
    bool operator<(const Job& other) const;
    struct Hash {
        size_t operator()(const Job& job) const;
    };
    // job fields and resource requirements
    int submission_id;
    int receipt_id;
    string sender;
    string name;
    string username;
    string executable;
    int priority;
    int time_required;
    int min_memory;
    int min_cores;
    int max_memory;
    int gpu_required;
    int time;
    int time_run;
    bool completed;
    vector<string> execution_list;
    string submit_time;
    string first_response;
    string receive_time;
    string submission_completion_time;
};

// helpers for reading the job config
Job get_job(const string& config_path);
string read_file(const string& config_path);
string extract_string(const string& json, const string& key);
int extract_int(const string& json, const string& key);
}
