#include "n2_protocol.h"
#include "n2_protocol.hpp"

#include <blackhole/attribute.hpp>
#include <fcntl.h>

#include "access_context.h"
#include "common.hpp"
#include "elliptics.h"
#include "old_protocol/old_protocol.hpp"

n2_message::n2_message(const dnet_cmd &cmd_)
: cmd(cmd_)
{}

n2_request::n2_request(const dnet_cmd &cmd_, const dnet_time &deadline_)
: n2_message(cmd_)
, deadline(deadline_)
{}

namespace ioremap { namespace elliptics { namespace n2 {

protocol_interface *net_state_get_protocol(dnet_net_state* st) {
	return &st->n->io->old_protocol->protocol;
}

static dnet_time default_deadline() {
	dnet_time res;
	dnet_empty_time(&res);
	return res;
}

lookup_request::lookup_request(const dnet_cmd &cmd_)
: n2_request(cmd_, default_deadline())
{}

lookup_response::lookup_response(const dnet_cmd &cmd_)
: n2_message(cmd_)
, record_flags(0)
, user_flags(0)
, path()
, json_timestamp({0, 0})
, json_offset(0)
, json_size(0)
, json_capacity(0)
, json_checksum()
, data_timestamp({0, 0})
, data_offset(0)
, data_size(0)
, data_checksum()
{}

lookup_response::lookup_response(const dnet_cmd &cmd_,
                                 uint64_t record_flags_,
                                 uint64_t user_flags_,
                                 std::string path_,
                                 dnet_time json_timestamp_,
                                 uint64_t json_offset_,
                                 uint64_t json_size_,
                                 uint64_t json_capacity_,
                                 std::vector<unsigned char> json_checksum_,
                                 dnet_time data_timestamp_,
                                 uint64_t data_offset_,
                                 uint64_t data_size_,
                                 std::vector<unsigned char> data_checksum_)
: n2_message(cmd_)
, record_flags(record_flags_)
, user_flags(user_flags_)
, path(std::move(path_))
, json_timestamp(json_timestamp_)
, json_offset(json_offset_)
, json_size(json_size_)
, json_capacity(json_capacity_)
, json_checksum(std::move(json_checksum_))
, data_timestamp(data_timestamp_)
, data_offset(data_offset_)
, data_size(data_size_)
, data_checksum(std::move(data_checksum_))
{}

}}} // namespace ioremap::elliptics::n2

extern "C" {

dnet_cmd *n2_request_info_get_cmd(struct n2_request_info *req_info) {
	return &req_info->request->cmd;
}

void n2_request_info_free(struct n2_request_info *req_info) {
	delete req_info;
}

void n2_response_info_free(struct n2_response_info *resp_info) {
	delete resp_info;
}

} // extern "C"
