#include "job.hpp"

#include <functional>
#include <string>
#include <unistd.h>

using namespace std;


namespace compute_jobs {
namespace {

// hostname
string get_username() {
    char hostname[256] = {};
    if (gethostname(hostname, sizeof(hostname)) == 0) {
        hostname[sizeof(hostname) - 1] = '\0';
        return string(hostname);
    }

    return "";
}

}

// a job received from the master
Job::Job(const string& name,
         const string& executable,
         int priority,
         int time_required,
         int min_memory,
         int min_cores,
         int max_memory,
         int gpu_required)
    : submission_id(-1),
      receipt_id(-1),
      sender(""),
      name(name),
      username(get_username()),
      executable(executable),
      priority(priority),
      time_required(time_required),
      min_memory(min_memory),
      min_cores(min_cores),
      max_memory(max_memory),
      gpu_required(gpu_required),
      time(0),
      time_run(0),
      completed(false),
      execution_list(),
      submit_time(""),
      first_response(""),
      receive_time(""),
      submission_completion_time("") {}

// executable name
string Job::get_executable_name() const {
    const size_t separator = executable.find_last_of('/');
    if (separator == string::npos) {
        return "/" + executable;
    }

    return executable.substr(separator);
}

// short form for logs
string Job::to_string() const {
    return "JOB: " + ::to_string(submission_id) + "," +
           ::to_string(receipt_id);
}

// same receipt, or same submission from the same sender
bool Job::operator==(const Job& other) const {
    return (receipt_id == other.receipt_id) ||
           (submission_id == other.submission_id &&
            sender == other.sender &&
            !sender.empty());
}

// order by priority
bool Job::operator<(const Job& other) const {
    return priority < other.priority;
}

// set sender
void Job::set_sender(const string& sender_name) {
    sender = sender_name;
}

// set submission id
void Job::set_submission_id(int id) {
    submission_id = id;
}
// set receipt id
void Job::set_receipt_id(int id) {
    receipt_id = id;
}
// record when execution started
void Job::start_execution(const string& started_at) {
    if (submit_time.empty()) {
        submit_time = started_at;
    }

    if (first_response.empty()) {
        first_response = started_at;
    }
}

// record execution step
void Job::record_execution_step(const string& step_name) {
    execution_list.push_back(step_name);
}

// record when execution finished
void Job::finish_execution(int total_runtime, const string& completed_at) {
    time_run = total_runtime;
    time = total_runtime;
    completed = true;
    submission_completion_time = completed_at;
}


// combines submission_id and job sender
size_t Job::Hash::operator()(const Job& job) const {
    return hash<string>{}(
        ::to_string(job.submission_id) + "|" + job.sender);
}

}
