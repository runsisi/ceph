// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace pool2 {

namespace at = argument_types;
namespace po = boost::program_options;

void get_arguments_stats(po::options_description *positional,
                         po::options_description *options) {
  at::add_pool_options(positional, options);
  at::add_format_options(options);
  options->add_options()
      ("count", po::value<int>(), "count");
}

int execute_stats(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name = utils::get_pool_name(vm, &arg_index);

  at::Format::Formatter formatter;
  int r = utils::get_formatter(vm, &formatter);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  int count = 1;
  if (vm.count("count")) {
      count = vm["count"].as<int>();
  }

  librbd::RBD rbd;
  librbd::PoolStats pool_stats;
  for (int i = 0; i < count; i++) {
    r = rbd.pool_stats_get(io_ctx, &pool_stats);
    if (r < 0) {
      std::cerr << "rbd: failed to query pool stats: " << cpp_strerror(r)
                << std::endl;
      return r;
    }
  }

  if (formatter) {
    formatter->open_object_section("stats");
    formatter->open_object_section("images");
    formatter->close_section();
    formatter->open_object_section("trash");
    formatter->close_section();
    formatter->close_section();

    formatter->flush(std::cout);
  }
  return 0;
}

Shell::Action stat_action(
  {"pool2", "stats"}, {}, "Display pool statistics.",
  "Note: legacy v1 images are not included in stats",
  &get_arguments_stats, &execute_stats);

} // namespace pool2
} // namespace action
} // namespace rbd
