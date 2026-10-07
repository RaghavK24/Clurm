#include "job.hpp"

#include <fstream>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>

using namespace std;

namespace client_jobs {


// read the whole config file
string read_file(const string& config_path) {
    ifstream input(config_path.c_str());
    if (!input) {
        throw runtime_error("Unable to open job config: " + config_path);
    }

    ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

// required string field
string extract_string(const string& json, const string& key) {
    const regex pattern("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"");
    smatch match;
    if (!regex_search(json, match, pattern)) {
        throw runtime_error("Missing string field: " + key);
    }

    return match[1].str();
}

// required integer field
int extract_int(const string& json, const string& key) {
    const regex pattern("\"" + key + "\"\\s*:\\s*(-?\\d+)");
    smatch match;
    if (!regex_search(json, match, pattern)) {
        throw runtime_error("Missing integer field: " + key);
    }

    return stoi(match[1].str());
}

// optional integer field
int extract_optional_int(const string& json, const string& key, int default_val) {
    const regex pattern("\"" + key + "\"\\s*:\\s*(-?\\d+)");
    smatch match;
    if (!regex_search(json, match, pattern)) {
        return default_val;
    }
    return stoi(match[1].str());
}



// read a job config into a Job
Job get_job(const string& config_path) {
    const string json = read_file(config_path);

    return Job(extract_string(json, "name"),
               extract_string(json, "executable"),
               extract_int(json, "priority"),
               extract_int(json, "time_required"),
               extract_int(json, "min_memory"),
               extract_int(json, "min_cores"),
               extract_int(json, "max_memory"),
               extract_optional_int(json, "gpu_required", 0));
}

}
