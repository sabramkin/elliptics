#ifndef ELLIPTICS_PROTOCOL_HPP
#define ELLIPTICS_PROTOCOL_HPP

#include <optional>

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

// While refactoring is in progress we use protocol structs as core structs
using dnet_cmd_native = dnet_cmd;

struct data_in_file {
	int	fd;
	off_t	local_offset;
	size_t	fsize;
	int 	on_exit;

	data_in_file() : fd(-1) {}
};

struct data_place {
	enum place_type {
		IN_MEMORY,
		IN_FILE,
	};

	data_in_file	in_file;
	data_pointer	in_memory;

	place_type where() const;

	static data_place from_file(const data_in_file &in_file);
	static data_place from_memory(const data_pointer &in_memory);
};

class isender;

}} // namespace ioremap::elliptics

namespace ell = ioremap::elliptics;

// All requests/responses must be inherited from common_request;
// common_request is visible from dnet_io_req. Since this struct
// is visible from C code, it is declared out of namespace.
struct common_request {
	ell::dnet_cmd_native		cmd;
	std::optional<ell::data_place>	data;

	virtual ~common_request() = default;

	// TODO: uncomment pure virtual
	void send(ell::isender &) {}/* = 0;*/ // visitor
};

namespace ioremap { namespace elliptics {

struct dnet_read_request : common_request {
	uint64_t ioflags;
	uint64_t read_flags;
	uint64_t data_offset;
	uint64_t data_size;

	dnet_time deadline;
};

struct dnet_read_response : common_request {
	uint64_t record_flags;
	uint64_t user_flags;

	dnet_time json_timestamp;
	uint64_t json_size;
	uint64_t json_capacity;
	uint64_t read_json_size;
	data_pointer json;

	dnet_time data_timestamp;
	uint64_t data_size;
	uint64_t read_data_offset;
	uint64_t read_data_size;
	data_place data;
};

struct dnet_write_request : common_request {
	uint64_t ioflags;
	uint64_t user_flags;
	dnet_time timestamp;

	uint64_t json_size;
	uint64_t json_capacity;
	dnet_time json_timestamp;
	data_pointer json;

	uint64_t data_offset;
	uint64_t data_size;
	uint64_t data_capacity;
	uint64_t data_commit_size;
	data_pointer data;

	uint64_t cache_lifetime;

	dnet_time deadline;
};

struct dnet_lookup_response : common_request {
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

struct dnet_remove_request : common_request {
	uint64_t ioflags;
	dnet_time timestamp;
};

struct dnet_bulk_read_request : common_request {
	std::vector<dnet_id> keys;
	uint64_t ioflags;
	uint64_t read_flags;

	dnet_time deadline;
};


struct dnet_bulk_remove_request : common_request {
	dnet_bulk_remove_request();
	explicit dnet_bulk_remove_request(const std::vector<dnet_id> &keys_in);

	explicit dnet_bulk_remove_request(const std::vector<std::pair<dnet_id, dnet_time>> &keys_in);
	bool is_valid() const;

	uint64_t ioflags{0};
	std::vector<dnet_id> keys;
	std::vector<dnet_time> timestamps;
};

struct dnet_iterator_request : common_request {
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

struct dnet_iterator_response : common_request {
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

struct dnet_server_send_request : common_request {
	std::vector<dnet_raw_id> keys;
	std::vector<int> groups;
	uint64_t flags;
	uint64_t chunk_size;
	uint64_t chunk_write_timeout;
	uint64_t chunk_commit_timeout;
	uint8_t chunk_retry_count;
};

void validate_json(const std::string &json);

}} // namespace ioremap::elliptics

#endif // ELLIPTICS_PROTOCOL_HPP
