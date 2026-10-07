#pragma once

#include <cstddef>
#include <string>
#include <vector>

using namespace std;


namespace master_jobs {

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
    string to_forward_payload() const;

    bool operator==(const Job& other) const;
    bool operator<(const Job& other) const;

    void set_sender(const string& sender_name);
    void set_submission_id(int id);
    void set_receipt_id(int id);

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

Job receive_job_from_client(const string& client_payload,
                            int submission_id,
                            int receipt_id,
                            const string& sender);

}
