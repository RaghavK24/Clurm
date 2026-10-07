#include "job.hpp"

#include <functional>
#include <string>
#include <unistd.h>

using namespace std;

namespace client_jobs {
namespace {

// hostname, used to identify the submitter
string get_username() {
    char hostname[256] = {};
    if (gethostname(hostname, sizeof(hostname)) == 0) {
        hostname[sizeof(hostname) - 1] = '\0';
        return string(hostname);
    }

    return "";
}

}

// define job
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

// hash on submission id and sender
size_t Job::Hash::operator()(const Job& job) const {
    return hash<string>{}(
        ::to_string(job.submission_id) + "|" + job.sender);
}

}
