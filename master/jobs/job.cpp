#include "job.hpp"

#include <functional>
#include <sstream>
#include <string>
#include <unistd.h>

using namespace std;


namespace master_jobs {
namespace {

string get_username() {
    char hostname[256] = {};
    if (gethostname(hostname, sizeof(hostname)) == 0) {
        hostname[sizeof(hostname) - 1] = '\0';
        return string(hostname);
    }

    return "";
}

string quote(const string& value) {
    return "\"" + value + "\"";
}

}  

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

string Job::get_executable_name() const {
    const size_t separator = executable.find_last_of('/');
    if (separator == string::npos) {
        return "/" + executable;
    }

    return executable.substr(separator);
}

string Job::to_string() const {
    return "JOB: " + ::to_string(submission_id) + "," +
           ::to_string(receipt_id);
}

string Job::to_forward_payload() const {
    ostringstream payload;
    payload << "{"
            << "\"submission_id\":" << submission_id << ","
            << "\"receipt_id\":" << receipt_id << ","
            << "\"sender\":" << quote(sender) << ","
            << "\"name\":" << quote(name) << ","
            << "\"username\":" << quote(username) << ","
            << "\"executable\":" << quote(executable);
    
    if (!executable_name.empty()) {
        payload << ",\"executable_name\":" << quote(executable_name);
    }
    if (!executable_b64.empty()) {
        payload << ",\"executable_b64\":" << quote(executable_b64);
    }
    
    payload << ",\"priority\":" << priority << ","
            << "\"time_required\":" << time_required << ","
            << "\"min_memory\":" << min_memory << ","
            << "\"min_cores\":" << min_cores << ","
            << "\"max_memory\":" << max_memory << ","
            << "\"gpu_required\":" << gpu_required
            << "}";
    return payload.str();
}

bool Job::operator==(const Job& other) const {
    return (receipt_id == other.receipt_id) ||
           (submission_id == other.submission_id &&
            sender == other.sender &&
            !sender.empty());
}

bool Job::operator<(const Job& other) const {
    return priority < other.priority;
}

void Job::set_sender(const string& sender_name) {
    sender = sender_name;
}

void Job::set_submission_id(int id) {
    submission_id = id;
}

void Job::set_receipt_id(int id) {
    receipt_id = id;
}

size_t Job::Hash::operator()(const Job& job) const {
    return hash<string>{}(
        ::to_string(job.submission_id) + "|" + job.sender);
}

}  
