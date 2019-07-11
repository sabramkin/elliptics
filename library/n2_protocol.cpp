#include "n2_protocol.h"
#include "n2_protocol.hpp"

#include <blackhole/attribute.hpp>
#include <fcntl.h>

#include "access_context.h"
#include "common.hpp"
#include "elliptics.h"
#include "native_protocol/native_protocol.hpp"

n2_request::n2_request(const dnet_cmd &cmd_,
                       const dnet_time &deadline_)
: cmd(cmd_)
, deadline(deadline_)
{}

n2_request::n2_request(const dnet_cmd &cmd_,
                       const dnet_time &deadline_,
                       std::shared_ptr<n2_body> body_)
: cmd(cmd_)
, deadline(deadline_)
, body(std::move(body_))
{}

n2_response_info::n2_response_info(const dnet_cmd &cmd_in, std::function<int ()> &&response_holder_in)
: cmd(cmd_in)
, response_holder(std::move(response_holder_in))
{
	cmd.flags |= DNET_FLAGS_REPLY;
}

namespace ioremap { namespace elliptics { namespace n2 {

int protocol_interface::on_request(dnet_net_state *st, std::unique_ptr<n2_request_info> request_info) {
	auto r = static_cast<dnet_io_req *>(calloc(1, sizeof(dnet_io_req)));
	if (!r)
		return -ENOMEM;

	r->io_req_type = DNET_IO_REQ_TYPED_REQUEST;
	r->request_info = request_info.release();

	r->st = dnet_state_get(st);
	dnet_schedule_io(st->n, r);

	return 0;
};

protocol_interface *net_state_get_protocol(dnet_net_state* st) {
	return &st->n->io->native_protocol->protocol;
}

dnet_time default_deadline() {
	dnet_time res;
	dnet_empty_time(&res);
	return res;
}

lookup_response::lookup_response()
: record_flags(0)
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

lookup_response::lookup_response(uint64_t record_flags_,
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
: record_flags(record_flags_)
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

remove_request::remove_request()
: ioflags(0)
, timestamp({0, 0})
{}

remove_request::remove_request(uint64_t ioflags_,
                               dnet_time timestamp_)
: ioflags(ioflags_)
, timestamp(timestamp_)
{}

}}} // namespace ioremap::elliptics::n2

extern "C" {

struct dnet_cmd *n2_io_req_get_cmd(struct dnet_io_req *r) {
	switch (r->io_req_type) {
	case DNET_IO_REQ_TYPED_REQUEST:
		return &r->request_info->request.cmd;
	case DNET_IO_REQ_TYPED_RESPONSE:
		return &r->response_info->cmd;
	default:
		return nullptr; // Must never reach this code
	}
}

dnet_cmd *n2_request_info_get_cmd(struct n2_request_info *req_info) {
	return &req_info->request.cmd;
}

int n2_io_req_set_request_backend_id(struct dnet_io_req *r, int backend_id) {
	if (r->io_req_type == DNET_IO_REQ_TYPED_REQUEST) {
		r->request_info->request.cmd.backend_id = backend_id;
		return 0;
	} else {
		return -EINVAL;
	}
}

void n2_request_info_free(struct n2_request_info *req_info) {
	delete req_info;
}

void n2_response_info_free(struct n2_response_info *resp_info) {
	delete resp_info;
}

struct n2_response_info *n2_response_info_create_from_error(struct dnet_cmd *cmd, struct n2_repliers *repliers,
                                                            int err) {
	auto response_holder = std::bind(repliers->on_reply_error, err, true);
	return new n2_response_info{ *cmd, std::move(response_holder) };
}

int n2_response_info_call_response(struct dnet_node *n, struct n2_response_info *response_info) {
	return c_exception_guard(response_info->response_holder, n, __FUNCTION__);
}

void n2_reply_error(struct n2_repliers *repliers, int error) {
	if (repliers->on_reply_error) {
		repliers->on_last_error(error);
	}
}

void n2_destroy_repliers(struct n2_repliers *repliers) {
	delete repliers;
}

} // extern "C"
