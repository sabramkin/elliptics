#ifndef ELLIPTICS_PROTOCOL_HPP
#define ELLIPTICS_PROTOCOL_HPP

#include <elliptics/packet.h>
#include <elliptics/utils.hpp>

namespace ioremap { namespace elliptics {

#define DNET_READ_FLAGS_JSON (1<<0)
#define DNET_READ_FLAGS_DATA (1<<1)

static inline const char *dnet_dump_read_flags(uint64_t flags)
{
	static __thread char buffer[256];
	static struct flag_info infos[] = {
		{ DNET_READ_FLAGS_JSON, "json" },
		{ DNET_READ_FLAGS_DATA, "data" },
	};

	dnet_flags_dump_raw(buffer, sizeof(buffer), flags, infos, sizeof(infos) / sizeof(infos[0]));

	return buffer;
}

//

struct dnet_id_native {
	uint8_t id[DNET_ID_SIZE];
	uint32_t group_id;
	uint64_t reserved;
};

struct dnet_cmd_native {
	dnet_id_native id;
	int status;
	int cmd;
	int backend_id;
	uint64_t trace_id;
	uint64_t flags;
	uint64_t trans;
	uint64_t size;
};

struct dnet_time_native {
	uint64_t tsec;
	uint64_t tnsec;
};

struct data_in_file {
	int fd;
	off_t local_offset;
	size_t fsize;
};

struct data_place {
	data_in_file in_file;
	data_pointer in_memory;

	bool is_in_memory();
};

//

struct dnet_read_request {
	uint64_t ioflags;
	uint64_t read_flags;
	uint64_t data_offset;
	uint64_t data_size;

	dnet_time deadline;
};

struct dnet_read_response {
	uint64_t record_flags;
	uint64_t user_flags;

	dnet_time json_timestamp;
	uint64_t json_size;
	uint64_t json_capacity;
	uint64_t read_json_size;
	data_place json;

	dnet_time data_timestamp;
	uint64_t data_size;
	uint64_t read_data_offset;
	uint64_t read_data_size;
	data_place data;
};

struct dnet_write_request {
	uint64_t ioflags;
	uint64_t user_flags;
	dnet_time timestamp;

	uint64_t json_size;
	uint64_t json_capacity;
	dnet_time json_timestamp;

	uint64_t data_offset;
	uint64_t data_size;
	uint64_t data_capacity;
	uint64_t data_commit_size;
	data_pointer data;

	uint64_t cache_lifetime;

	dnet_time deadline;
};

struct dnet_lookup_response {
	uint64_t record_flags;
	uint64_t user_flags;
	std::string path;

	dnet_time json_timestamp;
	uint64_t json_offset;
	uint64_t json_size;
	uint64_t json_capacity;
	std::vector<unsigned char> json_checksum;

	dnet_time data_timestamp;
	uint64_t data_offset;
	uint64_t data_size;
	std::vector<unsigned char> data_checksum;
};

struct dnet_remove_request {
	uint64_t ioflags;
	dnet_time timestamp;
};

struct dnet_bulk_read_request {
	std::vector<dnet_id> keys;
	uint64_t ioflags;
	uint64_t read_flags;

	dnet_time deadline;
};


struct dnet_bulk_remove_request {
	dnet_bulk_remove_request();
	explicit dnet_bulk_remove_request(const std::vector<dnet_id> &keys_in);

	explicit dnet_bulk_remove_request(const std::vector<std::pair<dnet_id, dnet_time>> &keys_in);
	bool is_valid() const;

	uint64_t ioflags{0};
	std::vector<dnet_id> keys;
	std::vector<dnet_time> timestamps;
};

struct dnet_iterator_request {
	dnet_iterator_request();
	dnet_iterator_request(uint32_t type, uint64_t flags,
	                      const std::vector<dnet_iterator_range> &key_range,
	                      const std::tuple<dnet_time, dnet_time> &time_range);

	uint64_t iterator_id;
	uint32_t action;
	uint32_t type;
	uint64_t flags;
	std::vector<dnet_iterator_range> key_ranges;
	std::tuple<dnet_time, dnet_time> time_range;
	std::vector<uint32_t> groups;
};

struct dnet_iterator_response {
	uint64_t iterator_id;
	dnet_raw_id key;
	int status;

	uint64_t iterated_keys;
	uint64_t total_keys;

	uint64_t record_flags;
	uint64_t user_flags;

	dnet_time json_timestamp;
	uint64_t json_size;
	uint64_t json_capacity;
	uint64_t read_json_size;

	dnet_time data_timestamp;
	uint64_t data_size;
	uint64_t read_data_size;
	uint64_t data_offset;
	uint64_t blob_id;
};

struct dnet_server_send_request {
	std::vector<dnet_raw_id> keys;
	std::vector<int> groups;
	uint64_t flags;
	uint64_t chunk_size;
	uint64_t chunk_write_timeout;
	uint64_t chunk_commit_timeout;
	uint8_t chunk_retry_count;
};

template<typename T>
data_pointer serialize(const T &value);

template<typename T>
void deserialize(const data_pointer &data, T &value, size_t &offset);

template<typename T>
void deserialize(const data_pointer &data, T &value) {
	size_t offset = 0;
	deserialize(data, value, offset);
}

void validate_json(const std::string &json);

}} // namespace ioremap::elliptics

#endif // ELLIPTICS_PROTOCOL_HPP
