#include "job.hpp"

#include <regex>
#include <stdexcept>
#include <string>

using namespace std;


namespace compute_jobs {
namespace {

// required string field
string get_string(const string& json, const string& key) {
    string search = "\"" + key + "\"";
    // throw if the key, the colon or the quotes are missing
    size_t pos = json.find(search);
    if (pos == string::npos) throw runtime_error("Missing string field: " + key);
    pos = json.find(':', pos + search.length());
    if (pos == string::npos) throw runtime_error("Missing string field: " + key);
    pos = json.find('"', pos);
    if (pos == string::npos) throw runtime_error("Missing string field: " + key);
    pos++;
    size_t end = pos;
    // find the closing quote, skipping escaped characters
    while (end < json.length() && json[end] != '"') {
        if (json[end] == '\\') end += 2;
        else end++;
    }
    if (end >= json.length()) throw runtime_error("Missing string field: " + key);
    return json.substr(pos, end - pos);
}

// required integer field
int get_int(const string& json, const string& key) {
    // throw if the key, the colon or the number is missing
    string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == string::npos) throw runtime_error("Missing integer field: " + key);
    pos = json.find(':', pos + search.length());
    if (pos == string::npos) throw runtime_error("Missing integer field: " + key);
    pos++;
    while (pos < json.length() && isspace(json[pos])) pos++;
    size_t end = pos;
    if (end < json.length() && json[end] == '-') end++;
    while (end < json.length() && isdigit(json[end])) end++;
    if (pos == end) throw runtime_error("Missing integer field: " + key);
    return stoi(json.substr(pos, end - pos));
}

// optional string field
string get_optional_string(const string& json, const string& key, const string& default_val) {
    string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == string::npos) return default_val;
    pos = json.find(':', pos + search.length());
    if (pos == string::npos) return default_val;
    pos = json.find('"', pos);
    if (pos == string::npos) return default_val;
    pos++;
    size_t end = pos;
    while (end < json.length() && json[end] != '"') {
        if (json[end] == '\\') end += 2;
        else end++;
    }
    if (end >= json.length()) return default_val;
    return json.substr(pos, end - pos);
}

// optional integer field
int get_optional_int(const string& json, const string& key, int default_val) {
    string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == string::npos) return default_val;
    pos = json.find(':', pos + search.length());
    if (pos == string::npos) return default_val;
    pos++;
    while (pos < json.length() && isspace(json[pos])) pos++;
    size_t end = pos;
    if (end < json.length() && json[end] == '-') end++;
    while (end < json.length() && isdigit(json[end])) end++;
    if (pos == end) return default_val;
    return stoi(json.substr(pos, end - pos));
}

}

// parse the job the master forwarded
Job receive_forwarded_job(const string& master_payload) {
    Job job(get_string(master_payload, "name"),
            get_string(master_payload, "executable"),
            get_int(master_payload, "priority"),
            get_int(master_payload, "time_required"),
            get_int(master_payload, "min_memory"),
            get_int(master_payload, "min_cores"),
            get_int(master_payload, "max_memory"),
            get_optional_int(master_payload, "gpu_required", 0));

    job.executable_name = get_optional_string(master_payload, "executable_name", "");
    job.executable_b64 = get_optional_string(master_payload, "executable_b64", "");

    job.set_submission_id(get_int(master_payload, "submission_id"));
    job.set_receipt_id(get_int(master_payload, "receipt_id"));
    job.set_sender(get_string(master_payload, "sender"));

    return job;
}

}
