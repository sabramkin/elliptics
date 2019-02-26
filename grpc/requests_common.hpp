#pragma once

#include <thread>
#include <utility>
#include <memory>

#include <blackhole/attribute.hpp>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "elliptics.grpc.fb.h"

#include "elliptics/utils.hpp"

#include "library/elliptics.h"
#include "library/logger.hpp"
#include "library/protocol.hpp"

namespace fb = flatbuffers;
namespace ell = ioremap::elliptics;

namespace grpc_dnet {

struct service_provider_info {
	dnet_node *node;
	grpc::ServerCompletionQueue *cq;
	fb_grpc_dnet::Elliptics::AsyncService *service;
};

class request_manager_base {
public:
	request_manager_base(service_provider_info *provider_in)
		: provider_(provider_in)
	{}

	virtual void process_completion(bool more) = 0;
	virtual ~request_manager_base() = default;

protected:
	enum : size_t {
		MAX_MESSAGE_SIZE = 4 * 1024 * 1024 /*4Mb*/ - 1024 /*some overhead*/
	};

protected:
	service_provider_info *provider_;
	grpc::ServerContext ctx_;
};

//
// util functions
//

fb::Offset<fb_grpc_dnet::Cmd> serialize_cmd(fb::grpc::MessageBuilder &builder,
                                            const ell::dnet_cmd_native &cmd);

void deserialize_cmd(const fb_grpc_dnet::Cmd *fb_cmd, ell::dnet_cmd_native &cmd);

void nanosec_to_dnet_time(uint64_t fb_time, dnet_time &time);

uint64_t dnet_time_to_nanosec(const dnet_time &time);

void time_point_to_dnet_time(const std::chrono::system_clock::time_point &time_point,
                             dnet_time &time);

} // namespace grpc_dnet
