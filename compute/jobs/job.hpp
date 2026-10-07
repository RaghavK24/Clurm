#pragma once

#include <cstddef>
#include <string>
#include <vector>

using namespace std;


namespace compute_jobs {

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

    void set_sender(const string& sender_name);
    void set_submission_id(int id);
    void set_receipt_id(int id);
    void start_execution(const string& started_at);
    void record_execution_step(const string& step_name);
    void finish_execution(int total_runtime, const string& completed_at);

    struct Hash {
        size_t operator()(const Job& job) const;
    };

    int submission_id;
    int receipt_id;
    string sender;
    string name;
    string username;
    string executable;
    string executable_name;
    string executable_b64;
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

Job receive_forwarded_job(const string& master_payload);
}
