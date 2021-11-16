#include <iostream>
#include <regex>

#include "clipp.h"
#include "replxx.hxx"

const std::string kPrompt = "\x1b[1;32mmykv\x1b[0m> ";
// words to be completed
const std::vector<std::string> kCommands = {
    "help", "quit", "exit", "clear", "get ", "set ", "rm ",
};

const std::vector<std::string> kCommandsHint = {
    "help",      "quit",     "exit", "clear", "get <key> <value>",
    "set <key>", "rm <key>",
};

const std::regex kCommandSetRegex("set\\s+(\\w+)\\s+(.+)\\s+");
const std::regex kCommandGetRegex("get\\s+(\\w+)\\s+");
const std::regex kCommandRmRegex("rm\\s+(\\w+)\\s+");

int main(int argc, char** argv) {
  std::string dbpath = "";
  bool check_crc = false;
  auto cli =
      (clipp::value("db path", dbpath), clipp::option("-c", "--checkcrc")
                                            .set(check_crc)
                                            .doc("Enable CRC verification"));
  if (!clipp::parse(argc, argv, cli)) {
    std::cout << clipp::make_man_page(cli, argv[0]);
    return 0;
  };

  // init the repl
  replxx::Replxx rx;

  std::cout << "Welcome to Mykv" << std::endl
            << "Press 'tab' to view autocompletions" << std::endl
            << "Type 'help' for help" << std::endl
            << "Type 'quit' or 'exit' to exit" << std::endl;

  rx.set_completion_callback([](std::string const& context, int& contextLen) {
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

  rx.set_hint_callback([](std::string const& context, int& contextLen,
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
      std::cout << "set: " << sm[1] << sm[2] << std::endl;
    } else if (std::regex_match(input, sm, kCommandGetRegex)) {
      std::cout << "get: " << sm[1] << std::endl;
    } else if (std::regex_match(input, sm, kCommandRmRegex)) {
      std::cout << "rm: " << sm[1] << std::endl;
    }
  }
}