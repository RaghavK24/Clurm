#pragma once

#include <fstream>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <iostream>

namespace common {
    // read a whole file
    inline std::string read_file(const std::string& path) {
        std::ifstream input(path.c_str());
        if (!input) {
            std::cerr << " No configuration file found at " << path << std::endl;
            return "";
        }
        std::ostringstream buffer;
        buffer << input.rdbuf();
        return buffer.str();
    }

    // string value for a key, or default_val if missing
    inline std::string extract_string(const std::string& json, const std::string& key, const std::string& default_val = "") {
        if (json.empty()) return default_val;
        std::string search = "\"" + key + "\"";
        size_t pos = json.find(search);
        if (pos == std::string::npos) return default_val;
        pos = json.find(':', pos + search.length());
        if (pos == std::string::npos) return default_val;
        pos = json.find('"', pos);
        if (pos == std::string::npos) return default_val;
        pos++;
        size_t end = pos;
        while (end < json.length() && json[end] != '"') {
            if (json[end] == '\\') end += 2;
            else end++;
        }
        if (end >= json.length()) return default_val;
        return json.substr(pos, end - pos);
    }
    // integer value for a key, or default_val if missing
    inline int extract_int(const std::string& json, const std::string& key, int default_val = 0) {
        if (json.empty()) return default_val;
        std::string search = "\"" + key + "\"";
        size_t pos = json.find(search);
        if (pos == std::string::npos) return default_val;
        pos = json.find(':', pos + search.length());
        if (pos == std::string::npos) return default_val;
        pos++;
        while (pos < json.length() && isspace(json[pos])) pos++;
        size_t end = pos;
        if (end < json.length() && json[end] == '-') end++;
        while (end < json.length() && isdigit(json[end])) end++;
        if (pos == end) return default_val;
        return std::stoi(json.substr(pos, end - pos));
    }

}
