#include "requests_common.hpp"


namespace grpc_dnet {

fb::Offset<fb_grpc_dnet::Cmd> serialize_cmd(fb::grpc::MessageBuilder &builder, ioremap::elliptics::dnet_cmd_native &cmd) {
	return fb_grpc_dnet::CreateCmd(
		builder,
		fb_grpc_dnet::CreateIdDirect(
			builder,
			&cmd.id.id,
			cmd.id.group_id
		),
		cmd.status,
		cmd.cmd,
		cmd.backend_id,
		cmd.trace_id,
		cmd.flags,
		cmd.trans,
		cmd.size
	);
}


void deserialize_cmd(const fb_grpc_dnet::Cmd *fb_cmd, ioremap::elliptics::dnet_cmd_native &cmd) {
	auto fb_cmd_id = fb_cmd->id();
	auto fb_cmd_id_id = fb_cmd_id->id();
	auto id_len = fb::VectorLength(fb_cmd_id_id);
	if (id_len) {
		if (id_len != DNET_ID_SIZE) {
			throw std::invalid_argument("Unexpected cmd.id size");
		}

		cmd.id.id.resize(DNET_ID_SIZE);
		memcpy(cmd.id.id.data(), fb_cmd_id->id(), DNET_ID_SIZE);

	} else {
		cmd.id.id.clear();
	}

	cmd.id.group_id = fb_cmd_id->group_id();

	cmd.status = fb_cmd->status();
	cmd.cmd = fb_cmd->cmd();
	cmd.backend_id = fb_cmd->backend_id();
	cmd.trace_id = fb_cmd->trace_id();
	cmd.flags = fb_cmd->flags();
	cmd.trans = fb_cmd->trans();
	cmd.size = fb_cmd->size();
}


void nanosec_to_dnet_time(uint64_t fb_time, ioremap::elliptics::dnet_time_native &time) {
	time.tsec = fb_time / 1000000000;
	time.tnsec = fb_time % 1000000000;
}


uint64_t dnet_time_to_nanosec(const ioremap::elliptics::dnet_time_native &time) {
	return time.tsec * 1000000000 + time.tnsec;
}


void time_point_to_dnet_time(const std::chrono::system_clock::time_point &time_point, ioremap::elliptics::dnet_time_native &time) {
	uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time_point.time_since_epoch()).count();
	nanosec_to_dnet_time(ns, time);
}

} // namespace grpc_dnet
