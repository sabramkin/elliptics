#include <thread>
#include <utility>
#include <memory>

#include <blackhole/attribute.hpp>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "elliptics.grpc.fb.h"

#include "../library/elliptics.h"
#include "../library/logger.hpp"


namespace fb = flatbuffers;

namespace grpc_dnet {

struct service_provider_info {
	dnet_node *node_;
	ell_grpc::Elliptics::AsyncService *service_;
	grpc::ServerCompletionQueue *cq_;
};


class request_manager_base {
public:
	request_manager_base(service_provider_info *service_provider_info__)
		: service_provider_info_(service_provider_info__)
		, in_reading_state_(true)
		, utilization_started_(false)
	{}

	virtual void process_completion(bool more) = 0;
	virtual ~request_manager_base() = default;

protected:
	service_provider_info *service_provider_info_;
	grpc::ServerContext ctx_;
	bool in_reading_state_;
	bool utilization_started_;
};


//
// util functions
//

void deserialize_cmd(const fb_grpc_dnet::Cmd *fb_cmd, ioremap::elliptics::dnet_cmd_native &cmd);

void nanosec_to_dnet_time(uint64_t fb_time, ioremap::elliptics::dnet_time_native &time);
uint64_t dnet_time_to_ns(const ioremap::elliptics::dnet_time_native &time);

void time_point_to_dnet_time(const std::chrono::system_clock::time_point &time_point, ioremap::elliptics::dnet_time_native &time);

} // namespace grpc_dnet
