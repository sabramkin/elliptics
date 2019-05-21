#include "serialize.hpp"

#include <msgpack.hpp>

#include "library/elliptics.h"

namespace msgpack {

using namespace ioremap::elliptics;

template <typename Stream>
inline msgpack::packer<Stream> &operator <<(msgpack::packer<Stream> &o, const dnet_time &v) {
	o.pack_array(2);
	o.pack_fix_uint64(v.tsec);
	o.pack_fix_uint64(v.tnsec);
	return o;
}

template <typename Stream>
inline msgpack::packer<Stream> &operator<<(msgpack::packer<Stream> &o, const n2::lookup_response &v) {
	o.pack_array(12);

	o.pack(v.record_flags);
	o.pack(v.user_flags);
	o.pack(v.path);

	o.pack(v.json_timestamp);
	o.pack(v.json_offset);
	o.pack(v.json_size);
	o.pack(v.json_capacity);

	o.pack(v.data_timestamp);
	o.pack(v.data_offset);
	o.pack(v.data_size);

	o.pack(v.json_checksum);
	o.pack(v.data_checksum);

	return o;
}

} // namespace msgpack

namespace ioremap { namespace elliptics { namespace n2 {

void enqueue_net(dnet_net_state *st, std::unique_ptr<n2_serialized> serialized) {
	auto r = static_cast<dnet_io_req *>(calloc(1, sizeof(dnet_io_req)));
	if (!r)
		throw std::bad_alloc();

	r->serialized = serialized.release();
	n2_io_req_enqueue_net(st, r);
}

static uint64_t response_transform_flags(uint64_t flags) {
	return (flags & ~(DNET_FLAGS_NEED_ACK)) | DNET_FLAGS_REPLY;
}

std::unique_ptr<n2_serialized> serialize_error_response(const dnet_cmd &cmd_in) {
	dnet_cmd cmd = cmd_in;
	cmd.flags = response_transform_flags(cmd.flags);
	return std::unique_ptr<n2_serialized>(new n2_serialized{ cmd, {} });
}

std::unique_ptr<n2_serialized> serialize_lookup_request(std::unique_ptr<n2_request> msg_in) {
	auto &msg = static_cast<lookup_request &>(*msg_in);
	return std::unique_ptr<n2_serialized>(new n2_serialized{ msg.cmd, {} });
}

std::unique_ptr<n2_serialized> serialize_lookup_response(std::unique_ptr<n2_message> msg_in) {
	auto &msg = static_cast<lookup_response &>(*msg_in);

	msgpack::sbuffer msgpack_buffer;
	msgpack::pack(msgpack_buffer, msg);

	dnet_cmd cmd = msg.cmd;
	cmd.flags = response_transform_flags(cmd.flags);
	cmd.size = msgpack_buffer.size();

	return std::unique_ptr<n2_serialized>(
		new n2_serialized{ cmd, { data_pointer::copy(msgpack_buffer.data(), msgpack_buffer.size()) }});
}

}}} // namespace ioremap::elliptics::n2
