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

static int serialize_body(dnet_node *, const lookup_response &body, n2_serialized::chunks_t &chunks) {
	msgpack::sbuffer msgpack_buffer;
	msgpack::pack(msgpack_buffer, body);
	chunks.emplace_back(data_pointer::copy(msgpack_buffer.data(), msgpack_buffer.size()));
	return 0;
}

static size_t calculate_body_size(const n2_serialized::chunks_t &chunks) {
	size_t size = 0;
	for (const auto &chunk : chunks) {
		size += chunk.size();
	}
	return size;
}

int serialize(dnet_node *, const dnet_cmd &cmd,
              std::unique_ptr<n2_serialized> &out_serialized) {
	out_serialized.reset(new n2_serialized{ cmd, {} });
	out_serialized->cmd.size = 0;
	return 0;
}

template<class TMessageBody>
int serialize(dnet_node *n, const dnet_cmd &cmd, const TMessageBody &body,
              std::unique_ptr<n2_serialized> &out_serialized) {
	out_serialized.reset(new n2_serialized{ cmd, {} });
	int err = serialize_body(n, body, out_serialized->chunks);
	if (err)
		return err;
	out_serialized->cmd.size = calculate_body_size(out_serialized->chunks);
	return 0;
}

template int serialize<lookup_response>(dnet_node *n, const dnet_cmd &cmd_in, const lookup_response &body,
                                        std::unique_ptr<n2_serialized> &out_serialized);

}}} // namespace ioremap::elliptics::n2
