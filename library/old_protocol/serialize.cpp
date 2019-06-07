#include "serialize.hpp"

#include <blackhole/attribute.hpp>
#include <msgpack.hpp>

#include "library/common.hpp"
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

int enqueue_net(dnet_net_state *st, std::unique_ptr<n2_serialized> serialized) {
	auto r = static_cast<dnet_io_req *>(calloc(1, sizeof(dnet_io_req)));
	if (!r)
		return -ENOMEM;

	r->serialized = serialized.release();
	dnet_io_req_enqueue_net(st, r);
	return 0;
}

static uint64_t response_transform_flags(uint64_t flags) {
	return (flags & ~(DNET_FLAGS_NEED_ACK)) | DNET_FLAGS_REPLY;
}

int serialize_error_response(dnet_net_state *st, const dnet_cmd &cmd_in,
                             std::unique_ptr<n2_serialized> &out_serialized) {
	dnet_cmd cmd = cmd_in;
	cmd.flags = response_transform_flags(cmd.flags);

	out_serialized.reset(new n2_serialized{ cmd, {} });
	return 0;
}

int serialize_lookup_request(dnet_net_state *st, std::unique_ptr<n2_request> msg_in,
                             std::unique_ptr<n2_serialized> &out_serialized) {
	auto &msg = static_cast<lookup_request &>(*msg_in);

	out_serialized.reset(new n2_serialized{ msg.cmd, {} });
	return 0;
}

int serialize_lookup_response(dnet_net_state *st, std::unique_ptr<n2_message> msg_in,
                              std::unique_ptr<n2_serialized> &out_serialized) {
	auto impl = [&] {
		auto &msg = static_cast<lookup_response &>(*msg_in);

		msgpack::sbuffer msgpack_buffer;
		msgpack::pack(msgpack_buffer, msg);

		dnet_cmd cmd = msg.cmd;
		cmd.flags = response_transform_flags(cmd.flags);
		cmd.size = msgpack_buffer.size();

		out_serialized.reset(new n2_serialized{
			cmd, { data_pointer::copy(msgpack_buffer.data(), msgpack_buffer.size()) }
		});

		return 0;
	};

	return c_exception_guard(impl, st->n, __FUNCTION__);
}

}}} // namespace ioremap::elliptics::n2
