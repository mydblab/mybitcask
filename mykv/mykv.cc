
#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>
#include "clipp.h"
#include "mybitcask/mybitcask.h"
#include "replxx.hxx"

const std::string kPrompt = "\x1b[1;32mmykv\x1b[0m> ";
// words to be completed
const std::vector<std::string> kCommands = {
    "help", "quit", "exit", "clear", "get ", "set ", "rm ",
};

const std::vector<std::string> kCommandsHint = {
    "help",     "quit", "exit", "clear", "get <key>", "set <key> <value>",
    "rm <key>",
};

const std::regex kCommandSetRegex("set\\s+(\\w+)\\s+(.+)\\s*");
const std::regex kCommandGetRegex("get\\s+(\\w+)\\s*");
const std::regex kCommandRmRegex("rm\\s+(\\w+)\\s*");

const std::string kNilOutput = "(nil)";

using syntax_highlight_t =
    std::vector<std::pair<std::string, replxx::Replxx::Color>>;
using keyword_highlight_t =
    std::unordered_map<std::string, replxx::Replxx::Color>;

const keyword_highlight_t kWordColor{
    {"help", replxx::Replxx::Color::YELLOW},
    {"quit", replxx::Replxx::Color::YELLOW},
    {"exit", replxx::Replxx::Color::YELLOW},
    {"clear", replxx::Replxx::Color::YELLOW},
    {"get", replxx::Replxx::Color::YELLOW},
    {"set", replxx::Replxx::Color::YELLOW},
    {"rm", replxx::Replxx::Color::YELLOW},
};

const syntax_highlight_t kRegexColor{
    {"\".*?\"", replxx::Replxx::Color::BROWN},  // double quotes
    {"\'.*?\'", replxx::Replxx::Color::BROWN},  // single quotes
};

int utf8str_codepoint_len(char const* s, int utf8len);
void setup_replxx(replxx::Replxx& rx);
std::ostream& error() { return std::cerr << "(error): "; }

int main(int argc, char** argv) {
  std::string dbpath;
  bool check_crc = false;
  std::uint32_t dead_bytes_threshold = 128 * 1024 * 1024;

  auto cli = (clipp::value("db path", dbpath),
              clipp::option("-c", "--checksum")
                  .set(check_crc)
                  .doc("Enable CRC verification"),
              clipp::option("-d", "--dead_bytes_threshold")
                  .set(dead_bytes_threshold)
                  .doc("maximum single log file size"));
  if (!clipp::parse(argc, argv, cli)) {
    std::cerr << clipp::make_man_page(cli, argv[0]);
    return 0;
  };
  auto db = mybitcask::Open(dbpath, dead_bytes_threshold, check_crc);
  if (!db.ok()) {
    error() << "Unable to start mybitask. error: " << db.status() << std::endl;
    return 0;
  }

  // init the repl
  replxx::Replxx rx;
  setup_replxx(rx);

  std::cout << "Welcome to Mykv" << std::endl
            << "Press 'tab' to view autocompletions" << std::endl
            << "Type 'help' for help" << std::endl
            << "Type 'quit' or 'exit' to exit" << std::endl;

  while (true) {
    char const* cinput = nullptr;
    do {
      cinput = rx.input(kPrompt);
    } while ((cinput == nullptr) && (errno == EAGAIN));

    if (cinput == nullptr) {
      break;
    }
    std::string input(cinput);
    rx.history_add(input);

    if (input.empty()) {
      continue;
    }
    if (input == "quit" || input == "exit") {
      break;
    }
    if (input == "clear") {
      rx.clear_screen();
    } else if (input == "help") {
      std::cout << "set <key> <value>"
                << "\tSet the value of a string key to a string" << std::endl
                << "get <key>"
                << "\t\tGet the string value of a given string key" << std::endl
                << "rm <key>"
                << "\t\tRemove a given key" << std::endl
                << "help"
                << "\t\t\tDisplays the help output" << std::endl
                << "quit"
                << "\t\t\tExit the repl" << std::endl
                << "exit"
                << "\t\t\tExit the repl" << std::endl
                << "clear"
                << "\t\t\tClear the screen" << std::endl;
      continue;
    }
    std::smatch sm;
    if (std::regex_match(input, sm, kCommandSetRegex)) {
      auto status = (*db)->Insert(sm.str(1), sm.str(2));
      if (!status.ok()) {
        error() << status << std::endl;
      }
    } else if (std::regex_match(input, sm, kCommandGetRegex)) {
      std::string v;
      auto found = (*db)->Get(sm.str(1), &v);
      if (!found.ok()) {
        error() << found.status() << std::endl;
      } else if (*found) {
        std::cout << v << std::endl;
      } else {
        std::cout << kNilOutput << std::endl;
      }
    } else if (std::regex_match(input, sm, kCommandRmRegex)) {
      auto status = (*db)->Delete(sm.str(1));
      if (!status.ok()) {
        error() << status << std::endl;
      }
    } else {
      error() << "Unrecognized command." << std::endl;
    }
  }
}

void setup_replxx(replxx::Replxx& rx) {
  rx.set_completion_callback([&](std::string const& context, int& contextLen) {
    replxx::Replxx::completions_t completions;
    if (contextLen == 0) {
      return completions;
    }
    for (auto const& c : kCommands) {
      if (c.rfind(context, 0) == 0) {
        completions.emplace_back(c.c_str());
      }
    }
    return completions;
  });

  rx.set_hint_callback([&](std::string const& context, int& contextLen,
                           replxx::Replxx::Color& color) {
    replxx::Replxx::hints_t hints;
    if (contextLen == 0) {
      return hints;
    }
    for (auto const& c : kCommandsHint) {
      if (c.rfind(context, 0) == 0) {
        hints.emplace_back(c.c_str());
      }
    }
    return hints;
  });

  rx.set_highlighter_callback(
      [&](std::string const& context, replxx::Replxx::colors_t& colors) {
        for (auto const& e : kRegexColor) {
          size_t pos = 0;
          std::string str = context;
          std::smatch match;

          while (std::regex_search(str, match, std::regex(e.first))) {
            std::string c = match[0];
            std::string prefix = match.prefix().str();
            pos += utf8str_codepoint_len(prefix.c_str(),
                                         static_cast<int>(prefix.length()));
            int len =
                utf8str_codepoint_len(c.c_str(), static_cast<int>(c.length()));

            for (int i = 0; i < len; ++i) {
              colors.at(pos + i) = e.second;
            }

            pos += len;
            str = match.suffix();
          }
        }

        std::string command = context.substr(0, context.find(" "));
        auto it = kWordColor.find(command);
        if (it != kWordColor.end()) {
          for (int i = 0; i < command.size(); ++i) {
            colors.at(i) = (*it).second;
          }
        }
      });
}

int utf8str_codepoint_len(char const* s, int utf8len) {
  int codepointLen = 0;
  unsigned char m4 = 128 + 64 + 32 + 16;
  unsigned char m3 = 128 + 64 + 32;
  unsigned char m2 = 128 + 64;
  for (int i = 0; i < utf8len; ++i, ++codepointLen) {
    char c = s[i];
    if ((c & m4) == m4) {
      i += 3;
    } else if ((c & m3) == m3) {
      i += 2;
    } else if ((c & m2) == m2) {
      i += 1;
    }
  }
  return (codepointLen);
}
