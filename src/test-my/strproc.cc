/*
 * strproc.cc
 *
 *  Created on: Apr 26, 2018
 *      Author: runsisi
 */

#include <iostream>
#include <string>
#include <list>

using namespace std;

string test = string(
    "abc\n"
    "   \n"
    " id:1 usage1:101111 \t total1:1000\n"
    "\t\n"
    "\tid:23 usage23:200 total234:200\n"
    "\n"
    "\t"
    ""
    "id:345 \t usage345:30002 total:300000\n"
    " id:45678 \t usage4321:43 total:40000"
);

std::string::size_type scan_line(std::string *text, std::string *line) {
  if (text->empty()) {
    return std::string::npos;
  }

  auto r = text->find("\n");
  if (r == std::string::npos) {
    *line = *text;
    text->clear();
    return 0;
  }

  *line = text->substr(0, r);
  text->erase(0, r + 1);
  return 0;
}

void strip_line(std::string *line, std::string strip=" \t") {
  auto r = line->find_first_not_of(strip);
  if (r == std::string::npos) {
    line->clear();
    return;
  }

  line->erase(0, r);

  r = line->find_last_not_of(strip);
  *line = line->substr(0, r + 1);
}

std::list<string> split_line(const std::string *line, std::string split=" \t") {
  std::list<string> records;
  std::string::size_type pos = 0;

  while (true) {
    auto r = line->find_first_of(split, pos);
    if (r == std::string::npos) {
      records.push_back(line->substr(pos));
      break;
    }

    records.push_back(line->substr(pos, r - pos));

    pos = line->find_first_not_of(split, r);
  }

  return records;
}

int main() {
  string line;

  while (std::string::npos != scan_line(&test, &line)) {
    cout << "---------------------------\n";
    cout << "scanned line:" << line << endl;
    strip_line(&line);
    cout << "stripped line:" << line << endl;
    if (line.empty()) {
      cout << "empty after stripped" << endl;
    } else {
      auto records = split_line(&line);
      for (auto &i : records) {
        cout << "   record:" << i << endl;
        auto fields = split_line(&i, ":");
        for (auto &i : fields) {
          cout << "    field:" << i << endl;
        }
      }
    }
  }

  return 0;
}
