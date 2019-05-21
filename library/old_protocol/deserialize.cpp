#include "deserialize.hpp"

#include <chrono>
#include <msgpack.hpp>

#include "library/common.hpp"

namespace msgpack {

using namespace ioremap::elliptics;

inline dnet_time &operator >>(msgpack::object o, dnet_time &v) {
	if (o.type != msgpack::type::ARRAY || o.via.array.size != 2) {
		throw msgpack::type_error();
	}

	object *p = o.via.array.ptr;
	p[0].convert(&v.tsec);
	p[1].convert(&v.tnsec);
	return v;
}

inline n2::lookup_response &operator >>(msgpack::object o, n2::lookup_response &v) {
	if (o.type != msgpack::type::ARRAY || o.via.array.size < 10)
		throw msgpack::type_error();

	object *p = o.via.array.ptr;
	p[0].convert(&v.record_flags);
	p[1].convert(&v.user_flags);
	p[2].convert(&v.path);

	p[3].convert(&v.json_timestamp);
	p[4].convert(&v.json_offset);
	p[5].convert(&v.json_size);
	p[6].convert(&v.json_capacity);

	p[7].convert(&v.data_timestamp);
	p[8].convert(&v.data_offset);
	p[9].convert(&v.data_size);

	if (o.via.array.size > 11) {
		p[10].convert(&v.json_checksum);
		p[11].convert(&v.data_checksum);

		if ((!v.json_checksum.empty() && v.json_checksum.size() != DNET_CSUM_SIZE) ||
		    (!v.data_checksum.empty() && v.data_checksum.size() != DNET_CSUM_SIZE)) {
			throw std::runtime_error("Unexpected checksum size");
		}
	}

	return v;
}

} // namespace msgpack

namespace ioremap { namespace elliptics { namespace n2 {

template<typename T>
void unpack(const data_pointer &data, T &value, size_t &length_of_packed) {
	length_of_packed = 0;

	msgpack::unpacked msg;
	msgpack::unpack(&msg, data.data<char>(), data.size(), &length_of_packed);
	msg.get().convert(&value);
}

std::unique_ptr<lookup_request> deserialize_lookup_request(const dnet_cmd &cmd) {
	return std::unique_ptr<lookup_request>(new lookup_request(cmd));
}

std::unique_ptr<lookup_response> deserialize_lookup_response(const dnet_cmd &cmd, data_pointer &&message_buffer) {
	std::unique_ptr<lookup_response> msg(new lookup_response(cmd));

	size_t unused_length_of_packed;
	unpack(message_buffer, *msg, unused_length_of_packed);

	return msg;
}

}}} // namespace ioremap::elliptics::n2
