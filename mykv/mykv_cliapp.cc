#include "mybitcask/mybitcask.h"
#include "mykv.h"

#include <iostream>
#include "clipp.h"

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
    error() << "Unable to start mybitask. error: " << db.status()
              << std::endl;
    return 0;
  }

  RunREPL(db->get());
}
