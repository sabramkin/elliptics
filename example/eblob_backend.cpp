/*
 * Copyright 2015+ Kirill Smorodinnikov <shaitkir@gmail.com>
 *
 * This file is part of Elliptics.
 *
 * Elliptics is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Elliptics is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Elliptics.  If not, see <http://www.gnu.org/licenses/>.
 */

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <condition_variable>

#include <boost/scope_exit.hpp>

#include <blackhole/wrapper.hpp>

#include "example/eblob_backend.h"

#include "elliptics/packet.h"
#include "elliptics/backends.h"
#include "elliptics/newapi/session.hpp"

#include "library/protocol.hpp"
#include "library/msgpack_conv.hpp"
#include "library/elliptics.h"
#include "library/backend.h"
#include "library/request_queue.h"
#include "library/logger.hpp"
#include "library/access_context.h"

#include "monitor/measure_points.h"

#include "bindings/cpp/timer.hpp"

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"


// Checks for broken headers signature(s).
int blob_check_corrupted_stamp(void *buffer, size_t buffer_size) {
	// Should be `assert`, but as __func__ is public (mostly for testing),
	// it could be called occasionally with arbitrary buffer size.
	if (buffer_size < sizeof(eblob_disk_control) + sizeof(dnet_ext_list_hdr)) {
		return 0;
	}

	const eblob_disk_control *dc = static_cast<const eblob_disk_control *>(buffer);
	const dnet_ext_list_hdr *ehdr = reinterpret_cast<const dnet_ext_list_hdr *>(dc + 1);

	const auto check_dc_header = [] (const eblob_disk_control &dc) {
		constexpr auto flags_limit = 1 << 9;
		return dc.flags && dc.flags < flags_limit &&
		       dc.data_size &&
		       dc.disk_size >= dc.data_size &&
		       dc.position == 0;
	};

	const auto check_ext_header = [] (const dnet_ext_list_hdr &ehdr) {
		return ehdr.version == DNET_EXT_VERSION_V1 &&
		       ehdr.timestamp.tsec <= DNET_SERVER_SEND_BUGFIX_TIMESTAMP &&
		       !(ehdr.__pad1[0] || ehdr.__pad1[1] || ehdr.__pad1[2]) &&
		       !(ehdr.__pad2[0] || ehdr.__pad2[1]);
	};

	if (check_dc_header(*dc) && check_ext_header(*ehdr)) {
		return -EILSEQ;
	}

	return 0;
}

int blob_read_and_check_stamp(const eblob_backend_config *c,
                              const dnet_time *timestamp,
                              int fd,
                              uint64_t data_offset,
                              uint64_t data_size) {
	std::array<char, sizeof(eblob_disk_control) + sizeof(dnet_ext_list_hdr)> stamp;

	if (data_size < stamp.size()) {
		return 0;
	}

	if (!timestamp || timestamp->tsec > DNET_SERVER_SEND_BUGFIX_TIMESTAMP) {
		return 0;
	}

	int err = dnet_read_ll(fd, stamp.data(), stamp.size(), data_offset);
	if (err) {
		return err;
	}

	err = blob_check_corrupted_stamp(static_cast<void *>(stamp.data()), stamp.size());
	if (err == -EILSEQ) {
		HANDY_COUNTER_INCREMENT(("backend.%u.stamp_corruption", c->data.stat_id), 1);
	}

	return err;
}

static int dnet_get_filename(int fd, std::string &filename) {
	char *name = nullptr;
	if (const int err = dnet_fd_readlink(fd, &name) < 0)
		return err;

	filename.assign(name);
	free(name);
	return 0;
}

int dnet_blob_config_to_json(struct dnet_config_backend *b, char **json_stat, size_t *size) {
	struct eblob_backend_config *c = static_cast<struct eblob_backend_config *>(b->data);
	int err = 0;

	rapidjson::Document doc;
	doc.SetObject();
	rapidjson::Document::AllocatorType &allocator = doc.GetAllocator();

	doc.AddMember("blob_flags", c->data.blob_flags, allocator);
	doc.AddMember("sync", c->data.sync, allocator);
	if (c->data.file)
		doc.AddMember("data", c->data.file, allocator);
	else
		doc.AddMember("data", "", allocator);
	doc.AddMember("blob_size", c->data.blob_size, allocator);
	doc.AddMember("records_in_blob", c->data.records_in_blob, allocator);
	doc.AddMember("defrag_percentage", c->data.defrag_percentage, allocator);
	doc.AddMember("defrag_timeout", c->data.defrag_timeout, allocator);
	doc.AddMember("index_block_size", c->data.index_block_size, allocator);
	doc.AddMember("index_block_bloom_length", c->data.index_block_bloom_length, allocator);
	doc.AddMember("blob_size_limit", c->data.blob_size_limit, allocator);
	doc.AddMember("defrag_time", c->data.defrag_time, allocator);
	doc.AddMember("defrag_splay", c->data.defrag_splay, allocator);

	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	doc.Accept(writer);

	std::string json = buffer.GetString();

	*json_stat = (char *)malloc(json.length() + 1);
	if (*json_stat) {
		*size = json.length();
		snprintf(*json_stat, *size + 1, "%s", json.c_str());
	} else {
		err = -ENOMEM;
		goto err_out_reset;
	}

	return 0;

err_out_reset:
	*size = 0;
	*json_stat = NULL;
	return err;
}

int dnet_read_json_header(int fd, uint64_t offset, uint64_t size, dnet_json_header *jhdr) {
	memset(jhdr, 0, sizeof(*jhdr));

	if (!size)
		return 0;

	auto json_header = ioremap::elliptics::data_pointer::allocate(size);
	int err = dnet_read_ll(fd, (char *)json_header.data(), json_header.size(), offset);
	if (err)
		return err;

	try {
		deserialize(json_header, *jhdr);
	} catch( std::exception &) {
		return -EINVAL;
	}

	return 0;
}

static int blob_read_and_check_flags_new(const eblob_backend_config *c,
                                         eblob_key *key,
                                         eblob_write_control *wc) {
	int err = eblob_read_return(c->eblob, key, EBLOB_READ_NOCSUM, wc);
	if (err == 0 && wc->flags & BLOB_DISK_CTL_CORRUPTED) {
		err = -EILSEQ;
		HANDY_COUNTER_INCREMENT(("backend.%u.marked_corrupted", c->data.stat_id), 1);
	}
	if (err == 0 && wc->flags & BLOB_DISK_CTL_UNCOMMITTED)
		err = -ENOENT;
	return err;
}

int blob_file_info_new(eblob_backend_config *c, void *state, dnet_cmd *cmd, struct dnet_access_context *context) {
	using namespace ioremap::elliptics;

	if (context) {
		context->add({{"id", std::string(dnet_dump_id(&cmd->id))},
		              {"backend_id", c->data.stat_id},
		             });
	}

	eblob_key key;
	memcpy(key.id, cmd->id.id, EBLOB_ID_SIZE);

	eblob_write_control wc;
	int err = blob_read_and_check_flags_new(c, &key, &wc);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: failed: {} [{}]", dnet_dump_id(&cmd->id),
		               strerror(-err), err);
		return err;
	}

	std::string filename;
	err = dnet_get_filename(wc.data_fd, filename);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: dnet_get_filename: fd: {}:  failed: {} [{}]",
		               dnet_dump_id(&cmd->id), wc.data_fd, strerror(-err), err);
		return err;
	}

	dnet_ext_list_hdr ehdr;
	memset(&ehdr, 0, sizeof(ehdr));

	dnet_json_header jhdr;
	memset(&jhdr, 0, sizeof(jhdr));

	uint64_t json_offset = 0;

	if (wc.flags & BLOB_DISK_CTL_EXTHDR) {
		if (wc.total_data_size < sizeof(ehdr)) {
			err = -ERANGE;
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: invalid record: total_data_size({}) < "
			                        "ehdr({}) : {} [{}]",
			               dnet_dump_id(&cmd->id), wc.total_data_size, sizeof(ehdr), strerror(-err), err);
			return err;
		}

		err = dnet_ext_hdr_read(&ehdr, wc.data_fd, wc.data_offset);
		if (err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: failed to read ext header: {} [{}]",
			               dnet_dump_id(&cmd->id), strerror(-err), err);
			return err;
		}

		if (wc.total_data_size < sizeof(ehdr) + ehdr.size) {
			err = -ERANGE;
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: invalid record: total_data_size({}) < "
			                        "ehdr({}) + json_header({}) = {} : {} [{}]",
			               dnet_dump_id(&cmd->id), wc.total_data_size, sizeof(ehdr), ehdr.size,
			               sizeof(ehdr) + ehdr.size, strerror(-err), err);
			return err;
		}

		err = dnet_read_json_header(wc.data_fd, wc.data_offset + sizeof(ehdr), ehdr.size, &jhdr);
		if(err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: failed to read json header: {} [{}]",
			               dnet_dump_id(&cmd->id), strerror(-err), err);
			return err;
		}

		if (wc.total_data_size < sizeof(ehdr) + ehdr.size + jhdr.capacity) {
			err = -ERANGE;
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: invalid record: total_data_size({}) < "
			                        "ehdr({}) + json_header({}) + json capacity({}) = {} : {} [{}]",
			               dnet_dump_id(&cmd->id), wc.total_data_size, sizeof(ehdr), ehdr.size,
			               jhdr.capacity, sizeof(ehdr) + ehdr.size + jhdr.capacity, strerror(-err), err);
			return err;
		}

		json_offset = sizeof(ehdr) + ehdr.size;
	}

	const uint64_t json_size = jhdr.size;
	const uint64_t data_offset = json_offset + jhdr.capacity;
	const uint64_t data_size = (wc.size >= data_offset) ? (wc.size - data_offset) : 0;

	std::vector<unsigned char> json_checksum;
	std::vector<unsigned char> data_checksum;

	if (cmd->flags & DNET_FLAGS_CHECKSUM) {
		auto st = static_cast<struct dnet_net_state *>(state);

		auto verify_and_get_checksum = [&] (eblob_write_control wc, uint64_t offset, uint64_t size,
		                                    std::vector<unsigned char> &checksum, const char *csum_subject,
		                                    const char *csum_time_ctx_attr) {
			if (!size) {
				return 0;
			}

			wc.offset = offset;
			wc.size = size;

			util::steady_timer timer;

			BOOST_SCOPE_EXIT(&timer, &context, &csum_time_ctx_attr) {
				if (context) {
					uint64_t csum_time = timer.get_us();
					context->add({{csum_time_ctx_attr, csum_time}});
				}
			} BOOST_SCOPE_EXIT_END

			int err = eblob_verify_checksum(c->eblob, &key, &wc);
			if (err) {
				DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: {} checksum verification "
				               "failed: {} [{}]", dnet_dump_id(&cmd->id), csum_subject,
				               strerror(-err), err);
				return err;
			}

			checksum.resize(DNET_CSUM_SIZE);
			err = dnet_checksum_fd(st->n, wc.data_fd, wc.data_offset + offset, size,
			                       checksum.data(), checksum.size());

			if (err) {
				DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: failed to calculate {} "
				               "checksum: {} [{}]", dnet_dump_id(&cmd->id), csum_subject,
				               strerror(-err), err);
				return err;
			}

			return 0;
		};

		err = verify_and_get_checksum(wc, json_offset, json_size, json_checksum, "json", "json_csum_time");
		if (err) {
			return err;
		}

		err = verify_and_get_checksum(wc, data_offset, data_size, data_checksum, "data", "data_csum_time");
		if (err) {
			return err;
		}
	}

	dnet_lookup_response response;
	response.record_flags = wc.flags;
	response.user_flags = ehdr.flags;
	response.path = std::move(filename);
	response.json_timestamp = jhdr.timestamp;
	response.json_offset = wc.data_offset + json_offset;
	response.json_size = json_size;
	response.json_capacity = jhdr.capacity;
	response.json_checksum = std::move(json_checksum);
	response.data_timestamp = ehdr.timestamp;
	response.data_offset = wc.data_offset + data_offset;
	response.data_size = data_size;
	response.data_checksum = std::move(data_checksum);

	auto response_packed = serialize(std::move(response));

	err = dnet_send_reply(state, cmd, response_packed.data(), response_packed.size(), 0, context);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-file-info-new: dnet_send_reply: data: {:p}, size: {}: {} [{}]",
		               dnet_dump_id(&cmd->id), response_packed.data(), response_packed.size(),
		               strerror(-err), err);
		return err;
	}

	DNET_LOG_INFO(c->blog, "{}: EBLOB: blob-file-info-new: fd: {}, json_size: {}, data_size: {}",
	              dnet_dump_id(&cmd->id), wc.data_fd, json_size, data_size);

	return 0;
}

static int blob_del_new_cas(eblob_backend_config *c, eblob_backend *b, const dnet_cmd *cmd, eblob_key &key,
			    const ioremap::elliptics::dnet_remove_request &request) {
	eblob_write_control wc;
	int err = eblob_read_return(b, &key, EBLOB_READ_NOCSUM, &wc);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: {}: failed: {} [{}]", dnet_dump_id(&cmd->id),
			       __func__, strerror(-err), err);
		return err;
	}

	if (!(wc.flags & BLOB_DISK_CTL_EXTHDR)) {
		DNET_LOG_INFO(c->blog, "{}: EBLOB: {}: REMOVE_NEW: key doesn't have exthdr",
			      dnet_dump_id(&cmd->id), __func__);
		return 0;
	}

	auto verify_checksum = [&, wc] (uint64_t offset, uint64_t size) mutable {
		wc.offset = offset;
		wc.size = size;
		return eblob_verify_checksum(b, &key, &wc);
	};

	dnet_ext_list_hdr ehdr;
	err = verify_checksum(0, sizeof(ehdr));
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: {}: REMOVE_NEW: failed to verify checksum for "
			       "exthdr: fd: {}, offset: {}, size: {}: {} [{}]",
			       dnet_dump_id(&cmd->id), __func__, wc.data_fd, wc.offset, wc.size, strerror(-err), err);
		return 0;
	}

	err = dnet_ext_hdr_read(&ehdr, wc.data_fd, wc.data_offset);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: {}: REMOVE_NEW: exthdr read failed: {} [{}]",
			       dnet_dump_id(&cmd->id), __func__, strerror(-err), err);
		return err;
	}

	if (dnet_time_cmp(&ehdr.timestamp, &request.timestamp) > 0) {
		const std::string request_ts = dnet_print_time(&request.timestamp);
		const std::string disk_ts = dnet_print_time(&ehdr.timestamp);

		DNET_LOG_ERROR(c->blog, "{}: EBLOB: {}: REMOVE_NEW: failed cas: "
			       "data timestamp is greater than request timestamp: "
			       "disk-ts: {}, request-ts: {}",
			       dnet_dump_id(&cmd->id), __func__, disk_ts, request_ts);
		return -EBADFD;
	}

	return 0;
}

int blob_del_new(eblob_backend_config *c, dnet_cmd *cmd, void *data, struct dnet_access_context *context) {
	using namespace ioremap::elliptics;
	eblob_backend *b = c->eblob;

	const auto request = [&data, &cmd] () {
		dnet_remove_request request;
		deserialize(data_pointer::from_raw(data, cmd->size), request);
		return request;
	} ();

	if (context) {
		context->add({{"id", std::string(dnet_dump_id(&cmd->id))},
		              {"backend_id", c->data.stat_id},
		              {"ioflags", std::string(dnet_flags_dump_ioflags(request.ioflags))},
		             });
		if (request.ioflags & DNET_IO_FLAGS_CAS_TIMESTAMP) {
			context->add({"ts-cas", std::string(dnet_print_time(&request.timestamp))});
		}
	}

	DNET_LOG_INFO(c->blog, "{}: EBLOB: {}: REMOVE_NEW: start: ioflags: {}",
		      dnet_dump_id(&cmd->id), __func__, dnet_flags_dump_ioflags(request.ioflags));

	eblob_key key;
	memcpy(key.id, cmd->id.id, EBLOB_ID_SIZE);

	int err;
	if (request.ioflags & DNET_IO_FLAGS_CAS_TIMESTAMP) {
		err = blob_del_new_cas(c, b, cmd, key, request);
		if (err)
			return err;
	}

	err = eblob_remove(b, &key);

	DNET_LOG(c->blog, err ? DNET_LOG_ERROR : DNET_LOG_INFO, "{}: EBLOB: {} finished: {}",
		 dnet_dump_id(&cmd->id), __func__, dnet_print_error(err));

	return err;
}

static int blob_read_new_impl(eblob_backend_config *c,
                              void *state,
                              dnet_cmd *cmd,
                              dnet_cmd_stats *cmd_stats,
                              const ioremap::elliptics::dnet_read_request &request,
                              bool last_read,
                              dnet_access_context *context) {
	using namespace ioremap::elliptics;

	eblob_backend *b = c->eblob;

	DNET_LOG_NOTICE(c->blog, "{}: EBLOB: blob-read-new: {}: start: ioflags: {}, read_flags: {}, "
	                         "data_offset: {}, data_size: {}",
	                dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), dnet_flags_dump_ioflags(request.ioflags),
	                dnet_dump_read_flags(request.read_flags), request.data_offset, request.data_size);

	eblob_key key;
	memcpy(key.id, cmd->id.id, EBLOB_ID_SIZE);

	eblob_write_control wc;
	int err = blob_read_and_check_flags_new(c, &key, &wc);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: failed: {} [{}]", dnet_dump_id(&cmd->id),
		               strerror(-err), err);
		return err;
	}

	auto verify_checksum = [&, wc] (uint64_t offset, uint64_t size, uint64_t &csum_time) mutable {
		if (request.ioflags & DNET_IO_FLAGS_NOCSUM)
			return 0;
		wc.offset = offset;
		wc.size = size;
		util::steady_timer timer;
		const auto ret = eblob_verify_checksum(b, &key, &wc);
		csum_time = timer.get_us();
		return ret;
	};

	dnet_ext_list_hdr ehdr;
	memset(&ehdr, 0, sizeof(ehdr));

	dnet_json_header jhdr;
	memset(&jhdr, 0, sizeof(jhdr));

	uint64_t record_offset = 0;
	uint64_t headers_csum_time = 0;

	if (wc.flags & BLOB_DISK_CTL_EXTHDR) {
		if (wc.total_data_size < sizeof(ehdr)) {
			err = -ERANGE;
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: {}: invalid record: total_data_size({}) < "
			                        "ehdr({}) : {} [{}]",
			               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), wc.total_data_size,
			               sizeof(ehdr), strerror(-err), err);
			return err;
		}

		err = dnet_ext_hdr_read(&ehdr, wc.data_fd, wc.data_offset);
		if (err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: {}: failed to read ext header : {} [{}]",
			               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), strerror(-err), err);
			return err;
		}

		if (wc.total_data_size < sizeof(ehdr) + ehdr.size) {
			err = -ERANGE;
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: {}: invalid record: total_data_size({}) < "
			                        "ehdr({}) + json header({}) = {} : {} [{}]",
			               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), wc.total_data_size,
			               sizeof(ehdr), ehdr.size, sizeof(ehdr) + ehdr.size, strerror(-err), err);
			return err;
		}

		err = dnet_read_json_header(wc.data_fd, wc.data_offset + sizeof(ehdr), ehdr.size, &jhdr);
		if (err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: {}: failed to read json header : {} [{}]",
			               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), strerror(-err), err);
			return err;
		}

		if (wc.total_data_size < sizeof(ehdr) + ehdr.size + jhdr.capacity) {
			err = -ERANGE;
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: {}: invalid record: total_data_size({}) < "
			                        "ehdr({}) + json header({}) + json capacity({}) = {} : {} [{}]",
			               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), wc.total_data_size,
			               sizeof(ehdr), ehdr.size, jhdr.capacity, sizeof(ehdr) + ehdr.size + jhdr.capacity,
			               strerror(-err), err);
			return err;
		}

		/* verify headers' checksum only if the record has chunked checksum, otherwise
		 * it is too heavy operation.
		 */
		if (wc.flags & BLOB_DISK_CTL_CHUNKED_CSUM) {
			err = verify_checksum(0, sizeof(ehdr) + ehdr.size, headers_csum_time);
			if (err) {
				DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: {}: failed to verify checksum for headers: "
				                        "fd: {}, offset: {}, size: {}: {} [{}]",
				               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), wc.data_fd, wc.offset,
				               wc.size, strerror(-err), err);
				return err;
			}
		}

		wc.size -= sizeof(ehdr) + ehdr.size;
		wc.data_offset += sizeof(ehdr) + ehdr.size;
		record_offset += sizeof(ehdr) + ehdr.size;
	}

	uint64_t data_size = wc.size - jhdr.capacity;
	uint64_t data_offset = wc.data_offset + jhdr.capacity;

	err = blob_read_and_check_stamp(c, &ehdr.timestamp, wc.data_fd, data_offset, data_size);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new {}: corrupted signature: data offset {}, "
		               "data size {}",
		               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), data_offset, data_size);
		return err;
	}

	data_pointer json;
	uint64_t json_csum_time = 0;

	if (request.read_flags & DNET_READ_FLAGS_JSON && jhdr.size) {
		err = verify_checksum(record_offset, jhdr.size, json_csum_time);
		if (err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: {}: failed to verify checksum for "
			                        "json: fd: {}, offset: {}, size: {}: {} [{}]",
			               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), wc.data_fd, wc.offset,
			               wc.size, strerror(-err), err);
			return err;
		}

		json = data_pointer::allocate(jhdr.size);
		err = dnet_read_ll(wc.data_fd, (char*)json.data(), json.size(), wc.data_offset);
		if (err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: {}: failed to read json: fd: {}, "
			                        "offset: {}, size: {}: {} [{}]",
			               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), wc.data_fd, wc.data_offset,
			               json.size(), strerror(-err), err);
			return err;
		}
	}

	data_size = 0;
	data_offset = 0;
	uint64_t data_csum_time = 0;

	if (request.read_flags & DNET_READ_FLAGS_DATA) {
		data_size = wc.size - jhdr.capacity;
		data_offset = wc.data_offset + jhdr.capacity;

		if (request.data_offset && request.data_offset >= data_size) {
			err = -E2BIG;
			DNET_LOG_ERROR(c->blog,
			               "{}: EBLOB: blob-read-new: {}: requested offset({}) >= data_size({}): {} [{}]",
			               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), request.data_offset,
			               data_size, strerror(-err), err);
			return err;
		}

		data_size -= request.data_offset;
		data_offset += request.data_offset;
		record_offset += request.data_offset;

		if (request.data_size && request.data_size < data_size)
			data_size = request.data_size;

		err = verify_checksum(record_offset + jhdr.capacity, data_size, data_csum_time);
		if (err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: {}: failed to verify checksum for "
			                        "data: offset: {}, size: {}: {} [{}]",
			               dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd), request.data_offset,
			               request.data_size, strerror(-err), err);
			return err;
		}
	}

	cmd_stats->size = json.size() + data_size;

	dnet_read_response response;
	response.record_flags = wc.flags;
	response.user_flags = ehdr.flags;
	response.json_timestamp = jhdr.timestamp;
	response.json_size = jhdr.size;
	response.json_capacity = jhdr.capacity;
	response.read_json_size = json.size();
	response.data_timestamp = ehdr.timestamp;
	response.data_size = wc.size - jhdr.capacity;
	response.read_data_offset = request.data_offset;
	response.read_data_size = data_size;

	auto header = serialize(std::move(response));

	auto response_packed = data_pointer::allocate(sizeof(*cmd) + header.size() + json.size());
	memcpy(response_packed.data(), cmd, sizeof(*cmd));
	memcpy(response_packed.skip(sizeof(*cmd)).data(), header.data(), header.size());
	if (!json.empty())
		memcpy(response_packed.skip(sizeof(*cmd) + header.size()).data(), json.data(), json.size());

	response_packed.data<dnet_cmd>()->size = header.size() + json.size() + data_size;
	response_packed.data<dnet_cmd>()->flags |= DNET_FLAGS_REPLY | (last_read ? 0 : DNET_FLAGS_MORE);
	response_packed.data<dnet_cmd>()->flags &= ~DNET_FLAGS_NEED_ACK;

	cmd->flags &= ~DNET_FLAGS_NEED_ACK;
	err = dnet_send_fd((dnet_net_state *)state, response_packed.data(), response_packed.size(),
	                   wc.data_fd, data_offset, data_size, 0, context);

	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-read-new: dnet_send_reply: data {:p}, size: {}: {} [{}]",
		               dnet_dump_id(&cmd->id), response_packed.data(), response_packed.size(),
		               strerror(-err), err);
		return err;
	}

	if (context) {
		context->add({{"response_json_size", json.size()},
		              {"response_data_size", data_size},
		              {"header_csum_time", headers_csum_time},
		              {"json_csum_time", json_csum_time},
		              {"data_csum_time", data_csum_time},
		            });
	}

	DNET_LOG_INFO(c->blog, "{}: EBLOB: blob-read-new: json_size: {}, data_size: {}, headers_csum_time: {} usecs, "
	                       "json_csum_time: {} usecs, data_csum_time: {} usecs",
	              dnet_dump_id(&cmd->id), json.size(), data_size, headers_csum_time, json_csum_time,
	              data_csum_time);

	return 0;
}

int blob_read_new(eblob_backend_config *c,
                  void *state,
                  dnet_cmd *cmd,
                  void *data,
                  dnet_cmd_stats *cmd_stats,
                  dnet_access_context *context) {
	using namespace ioremap::elliptics;

	const auto request = [&data, &cmd] () {
		dnet_read_request request;
		deserialize(data_pointer::from_raw(data, cmd->size), request);
		return request;
	} ();

	if (context) {
		context->add({{"id", std::string(dnet_dump_id(&cmd->id))},
		              {"backend_id", c->data.stat_id},
		              {"read_flags", std::string(dnet_dump_read_flags(request.read_flags))},
		              {"ioflags", std::string(dnet_flags_dump_ioflags(request.ioflags))},
		            });
		if (request.read_flags & DNET_READ_FLAGS_DATA) {
			context->add({{"request_data_offset", request.data_offset},
			              {"request_data_size", request.data_size},
			             });
		}
	}

	return blob_read_new_impl(c, state, cmd, cmd_stats, request, true, context);
}

int blob_write_new(eblob_backend_config *c, void *state, dnet_cmd *cmd, void *data,
                   dnet_cmd_stats *cmd_stats, struct dnet_access_context *context) {
	using namespace ioremap::elliptics;

	struct eblob_backend *b = c->eblob;
	auto data_p = data_pointer::from_raw(data, cmd->size);

	auto request = [&data_p] () {
		size_t offset = 0;
		dnet_write_request request;
		deserialize(data_p, request, offset);
		data_p = data_p.skip(offset);
		return request;
	} ();

	if (context) {
		context->add({{"id", std::string(dnet_dump_id(&cmd->id))},
		              {"backend_id", c->data.stat_id},
		              {"ioflags", std::string(dnet_flags_dump_ioflags(request.ioflags))},
		              {"request_data_offset", request.data_offset},
		              {"request_data_size", request.data_size},
		              {"request_data_commit_size", request.data_commit_size},
		              {"request_data_capacity", request.data_capacity},
		              {"request_json_size", request.json_size},
		              {"request_json_capacity", request.json_capacity},
		              {"user_flags", to_hex_string(request.user_flags)},
		             });
	}

	cmd_stats->size = request.json_size + request.data_size;

	DNET_LOG_NOTICE(c->blog, "{}: EBLOB: blob-write-new: WRITE_NEW: start: ioflags: {}, json: {{size: {}, "
	                         "capacity: {}}}, data: {{offset: {}, size: {}, capacity: {}, commit_size: {}}}",
	                dnet_dump_id(&cmd->id), dnet_flags_dump_ioflags(request.ioflags), request.json_size,
	                request.json_capacity, request.data_offset, request.data_size, request.data_capacity,
	                request.data_commit_size);

	if (request.ioflags & DNET_IO_FLAGS_APPEND) {
		DNET_LOG_NOTICE(c->blog, "{}: EBLOB: blob-write-new: WRITE_NEW: append is not supported",
		                dnet_dump_id(&cmd->id));
		return -ENOTSUP;
	}

	bool record_exists = false;
	dnet_ext_list_hdr disk_ehdr;
	memset(&disk_ehdr, 0, sizeof(disk_ehdr));
	dnet_json_header disk_jhdr;
	memset(&disk_jhdr, 0, sizeof(disk_jhdr));
	eblob_write_control wc;
	memset(&wc, 0, sizeof(wc));
	eblob_key key;
	memcpy(key.id, cmd->id.id, EBLOB_ID_SIZE);

	auto read_ext_headers = [&] (dnet_ext_list_hdr &ehdr, dnet_json_header &jhdr) {
		record_exists = (eblob_read_return(b, &key, EBLOB_READ_NOCSUM, &wc) == 0);
		if (!record_exists)
			return;

		if (!(wc.flags & BLOB_DISK_CTL_EXTHDR))
			return;

		if (dnet_ext_hdr_read(&ehdr, wc.data_fd, wc.data_offset))
			return;

		if (ehdr.size)
			dnet_read_json_header(wc.data_fd, wc.data_offset + sizeof(ehdr), ehdr.size, &jhdr);
	};

	if (!(request.ioflags & DNET_IO_FLAGS_PREPARE) || (request.ioflags & DNET_IO_FLAGS_CAS_TIMESTAMP)) {
		read_ext_headers(disk_ehdr, disk_jhdr);
	}

	if (request.ioflags & DNET_IO_FLAGS_CAS_TIMESTAMP) {
		if (record_exists) {
			if (dnet_time_cmp(&disk_ehdr.timestamp, &request.timestamp) > 0) {
				const std::string request_ts = dnet_print_time(&request.timestamp);
				const std::string disk_ts = dnet_print_time(&disk_ehdr.timestamp);

				DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-write-new: WRITE_NEW: failed cas: "
				                        "data timestamp is greater than data to be written timestamp: "
				                        "disk-ts: {}, data-ts: {}",
				               dnet_dump_id(&cmd->id), disk_ts, request_ts);

				return -EBADFD;
			}

			if (disk_ehdr.size && dnet_time_cmp(&disk_jhdr.timestamp, &request.json_timestamp) > 0) {
				const std::string request_ts = dnet_print_time(&request.json_timestamp);
				const std::string disk_ts = dnet_print_time(&disk_jhdr.timestamp);

				DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-write-new: WRITE_NEW: failed cas: "
				                        "json timestamp is greater than json to be written timestamp: "
				                        "disk-ts: {}, json-ts: {}",
				               dnet_dump_id(&cmd->id), disk_ts, request_ts);

				return -EBADFD;
			}
		}
	}

	dnet_ext_list_hdr ehdr;
	memset(&ehdr, 0, sizeof(ehdr));
	ehdr.timestamp = request.timestamp;
	ehdr.flags = request.user_flags;

	dnet_json_header jhdr;
	memset(&jhdr, 0, sizeof(jhdr));
	if (request.json_capacity || request.json_size) {
		jhdr.size = request.json_size;
		jhdr.capacity = request.json_capacity;
		jhdr.timestamp = request.json_timestamp;
	}

	auto json_header = jhdr.capacity ? serialize(jhdr) : data_pointer();

	uint64_t flags = BLOB_DISK_CTL_EXTHDR;
	if (request.ioflags & DNET_IO_FLAGS_NOCSUM)
		flags |= BLOB_DISK_CTL_NOCSUM;

	int err = 0;

	if (request.ioflags & DNET_IO_FLAGS_PREPARE) {
		const uint64_t prepare_size =
		    sizeof(ehdr) + json_header.size() + request.json_capacity + request.data_capacity;
		err = eblob_write_prepare(b, &key, prepare_size, flags);
		if (err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-write-new: eblob_write_prepare: size: {} "
			                        "(json_capacity: {}, data_pointer: {}): {} [{}]",
			               dnet_dump_id(&cmd->id), prepare_size, request.json_capacity,
			               request.data_capacity, strerror(-err), err);
			return err;
		}
	} else {
		[&] () {
			if (!record_exists)
				return;

			if (request.ioflags & DNET_IO_FLAGS_UPDATE_JSON)
				ehdr.timestamp = disk_ehdr.timestamp;

			if (!disk_ehdr.size)
				return;

			jhdr = disk_jhdr;

			if (request.json_size || (request.ioflags & DNET_IO_FLAGS_UPDATE_JSON)) {
				jhdr.size = request.json_size;
				jhdr.timestamp = request.json_timestamp;
			}
		} ();
	}

	if (request.ioflags & DNET_IO_FLAGS_UPDATE_JSON) {
		/* update_json can not be applied to nonexistent or uncommitted records.
		 * we return -ENOENT in such cases.
		 */
		if (!record_exists || wc.flags & BLOB_DISK_CTL_UNCOMMITTED) {
			return -ENOENT;
		}
	} else if (!(request.ioflags & DNET_IO_FLAGS_PREPARE)) {
		/* plain_write and commit without prepare can not be applied to
		 * nonexistent or committed records.
		 * Return -ENOENT for nonexistent records and -EPERM for committed records.
		 */
		if  (!record_exists) {
			return -ENOENT;
		} else if(!(wc.flags & BLOB_DISK_CTL_UNCOMMITTED)) {
			return -EPERM;
		}
	}

	json_header = jhdr.capacity ? serialize(jhdr) : data_pointer();

	ehdr.size = json_header.size();

	std::vector<eblob_iovec> iov;
	iov.reserve(3);
	iov.emplace_back(eblob_iovec{&ehdr, sizeof(ehdr), 0});

	if (!json_header.empty()) {
		iov.emplace_back(eblob_iovec{json_header.data(), json_header.size(), sizeof(ehdr)});
	}

	if (request.json_size) {
		if (request.json_size > jhdr.capacity) {
			err = -E2BIG;
			DNET_LOG_ERROR(c->blog,
			               "{}: EBLOB: blob-write-new: WRITE_NEW: json ({}) exceed capacity ({}): {} [{}]",
			               dnet_dump_id(&cmd->id), request.json_size, jhdr.capacity, strerror(-err), err);
			return err;
		}
		const auto offset = sizeof(ehdr) + ehdr.size;
		iov.emplace_back(eblob_iovec{data_p.data(), request.json_size, offset});
	}

	if (request.data_size) {
		const auto offset = sizeof(ehdr) + ehdr.size + jhdr.capacity + request.data_offset;
		iov.emplace_back(eblob_iovec{data_p.skip(request.json_size).data(), request.data_size, offset});
	}

	if (request.ioflags & DNET_IO_FLAGS_PLAIN_WRITE) {
		err = eblob_plain_writev(b, &key, iov.data(), iov.size(), flags);
	} else if (request.ioflags & DNET_IO_FLAGS_UPDATE_JSON) {
		iov.emplace_back(eblob_iovec{nullptr, 0, wc.size});
		err = eblob_writev(b, &key, iov.data(), iov.size(), flags);
	} else {
		err = eblob_writev(b, &key, iov.data(), iov.size(), flags);
	}

	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-write-new: WRITE_NEW: writev failed {} [{}]",
		               dnet_dump_id(&cmd->id), strerror(-err), err);
		return err;
	}

	if (request.ioflags & DNET_IO_FLAGS_COMMIT) {
		const uint64_t commit_size = sizeof(ehdr) + ehdr.size + jhdr.capacity + request.data_commit_size;
		err = eblob_write_commit(b, &key, commit_size, flags);
		if (err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-write-new: eblob_write_commit: size: {}: {} [{}]",
			               dnet_dump_id(&cmd->id), commit_size, strerror(-err), err);
			return err;
		}
	}

	memset(&wc, 0, sizeof(wc));
	err = eblob_read_return(b, &key, EBLOB_READ_NOCSUM, &wc);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-write-new: eblob_read failed: {} [{}]", dnet_dump_id(&cmd->id),
		               strerror(-err), err);
		return err;
	}

	if (request.ioflags & DNET_IO_FLAGS_WRITE_NO_FILE_INFO) {
		cmd->flags |= DNET_FLAGS_NEED_ACK;
		return 0;
	}

	std::string filename;
	err = dnet_get_filename(wc.data_fd, filename);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-write-new: dnet_get_filename: fd: {}:  failed: {} [{}]",
		               dnet_dump_id(&cmd->id), wc.data_fd, strerror(-err), err);
		return err;
	}

	if (wc.size) {
		if (wc.size >= sizeof(ehdr) + ehdr.size) {
			wc.size -= sizeof(ehdr) + ehdr.size;
			wc.data_offset += sizeof(ehdr) + ehdr.size;
		} else
			return -EINVAL;
	}

	dnet_lookup_response response;
	response.record_flags = wc.flags;
	response.user_flags = ehdr.flags;
	response.path = filename;
	response.json_timestamp = jhdr.timestamp;
	response.json_offset = wc.data_offset;
	response.json_size = jhdr.size;
	response.json_capacity = jhdr.capacity;
	response.json_checksum = {};
	response.data_timestamp = ehdr.timestamp;
	response.data_offset = wc.data_offset + jhdr.capacity;
	response.data_size = wc.size ? (wc.size - jhdr.capacity) : 0;
	response.data_checksum = {};

	auto response_packed = serialize(std::move(response));

	err = dnet_send_reply(state, cmd, response_packed.data(), response_packed.size(), 0, context);
	if (err) {
		DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob-write-new: dnet_send_reply: data: {:p}, size: {}: {} [{}]",
		               dnet_dump_id(&cmd->id), (void *)response_packed.data(), response_packed.size(),
		               strerror(-err), err);
		return err;
	}

	DNET_LOG_INFO(c->blog, "{}: EBLOB: blob-write-new: ioflags: {}, json_size: {}, data_size: {}",
	              dnet_dump_id(&cmd->id), dnet_flags_dump_ioflags(request.ioflags), jhdr.size,
	              wc.size - jhdr.capacity);

	return 0;
}

static bool check_key_ranges(eblob_backend_config *c, ioremap::elliptics::dnet_iterator_request &request) {
	if (!(request.flags & DNET_IFLAGS_KEY_RANGE)) {
		return true;
	}

	request.flags &= ~DNET_IFLAGS_KEY_RANGE;

	if (request.key_ranges.empty()) {
		return true;
	}

	auto empty = [&] () {
		static const dnet_raw_id empty_key = {{0}};
		for (const auto &range : request.key_ranges) {
			if (memcmp(&empty_key, &range.key_begin, sizeof(empty_key)) ||
			    memcmp(&empty_key, &range.key_end, sizeof(empty_key))) {
				return false;
			}
		}
		return true;
	} ();

	if (empty) {
		DNET_LOG_ERROR(c->blog, "EBLOB: iterator: all keys in all ranges are 0");
		return true;
	}

	char k1[2 * DNET_ID_SIZE + 1];
	char k2[2 * DNET_ID_SIZE + 1];
	for (const auto &range : request.key_ranges) {
		if (dnet_id_cmp_str(range.key_begin.id, range.key_end.id) > 0) {
			DNET_LOG_ERROR(c->blog, "EBLOB: iterator: key_begin ({}) > key_end ({})",
			               dnet_dump_id_len_raw(range.key_begin.id, DNET_ID_SIZE, k1),
			               dnet_dump_id_len_raw(range.key_end.id, DNET_ID_SIZE, k2));
			return false;
		}
	}

	request.flags |= DNET_IFLAGS_KEY_RANGE;

	for (const auto &range : request.key_ranges) {
		DNET_LOG_NOTICE(c->blog, "EBLOB: iterator: using key range: {}...{}",
		                dnet_dump_id_len_raw(range.key_begin.id, DNET_ID_SIZE, k1),
		                dnet_dump_id_len_raw(range.key_end.id, DNET_ID_SIZE, k2));
	}

	return true;
}

static bool check_ts_range(eblob_backend_config *c, ioremap::elliptics::dnet_iterator_request &request) {
	if (!(request.flags & DNET_IFLAGS_TS_RANGE)) {
		return true;
	}

	request.flags &= ~DNET_IFLAGS_TS_RANGE;

	static const dnet_time empty_time{0, 0};
	if ((memcmp(&empty_time, &std::get<0>(request.time_range), sizeof(empty_time)) == 0) &&
	    (memcmp(&empty_time, &std::get<1>(request.time_range), sizeof(empty_time)) == 0)) {
		DNET_LOG_NOTICE(c->blog, "EBLOB: iterator: both times are zero");
		return true;
	}

	if (dnet_time_cmp(&std::get<0>(request.time_range), &std::get<1>(request.time_range)) > 0) {
		DNET_LOG_ERROR(c->blog, "EBLOB: iterator:  time_begin > time_end");
		return false;
	}

	request.flags |= DNET_IFLAGS_TS_RANGE;

	const std::string time_begin = dnet_print_time(&std::get<0>(request.time_range));
	const std::string time_end = dnet_print_time(&std::get<1>(request.time_range));

	DNET_LOG_NOTICE(c->blog, "EBLOB: iterator: using ts range: {}...{}", time_begin, time_end);
	return true;
}

struct iterated_key_info {
	iterated_key_info(const dnet_raw_id &key, const uint64_t record_flags, int fd)
	: key(key)
	, record_flags{record_flags}
	, fd{fd}
	, json_offset{0}
	, data_offset{0}
	, data_size{0} {
		memset(&jhdr, 0, sizeof(jhdr));
		memset(&ehdr, 0, sizeof(ehdr));
	}

	iterated_key_info(const eblob_disk_control *dc, int fd)
	: key{{0}}
	, record_flags{dc->flags}
	, fd{fd}
	, json_offset{0}
	, data_offset{0}
	, data_size{0} {
		memcpy(key.id, dc->key.id, DNET_ID_SIZE);

		memset(&jhdr, 0, sizeof(jhdr));
		memset(&ehdr, 0, sizeof(ehdr));
	}

	dnet_raw_id key;
	uint64_t record_flags;
	int fd;
	uint64_t json_offset;
	uint64_t data_offset;
	uint64_t data_size;
	dnet_json_header jhdr;
	dnet_ext_list_hdr ehdr;
};

typedef std::function<int (std::shared_ptr<iterated_key_info> info)> iterator_callback;

/*
 * \a congestion_control_monitor allows to limit amount of data sent to a remote backends simultaneously.
 */
class congestion_control_monitor
{
public:
	/*!
	 * Constructor: initializes internal state.
	 */
	congestion_control_monitor()
	: m_batch_size{MINIMAL_BATCH_SIZE}
	, m_bytes_pending{0}
	, m_bytes_processed{0}
	, m_need_restart_timer{true} {
	}

	/*!
	 * Waits until batch had any space available, then increments
	 * number of pending bytes in the batch.
	 */
	void add_bytes(uint64_t bytes) {
		std::unique_lock<std::mutex> lock{m_mutex};

		while (is_batch_full()) {
			m_cond.wait(lock);
		}
		m_bytes_pending += bytes;

		if (m_need_restart_timer) {
			m_batch_timer.restart();
			m_need_restart_timer = false;
		}
	}

	/*!
	 * Decrements number of pending bytes in the batch and measures
	 * elapsed time of processing of the batch for either increasing or decreasing size of
	 * the batch.
	 */
	void remove_bytes(uint64_t bytes) {
		std::unique_lock<std::mutex> lock{m_mutex};

		m_bytes_processed += bytes;

		if (m_bytes_processed >= m_batch_size) {
			m_bytes_processed -= m_batch_size;

			if (m_batch_timer.get_ms() < BATCH_TIMEOUT_MSEC) {
				m_batch_size *= 2;
			} else {
				m_batch_size = std::max(m_batch_size / 2, MINIMAL_BATCH_SIZE);
			}

			m_need_restart_timer = true;
		}

		m_bytes_pending -= bytes;

		if (!is_batch_full()) {
			m_cond.notify_one();
		}
	}

	/*!
	 * Waits until all bytes are processed
	 */
	void wait_completion() {
		std::unique_lock<std::mutex> lock{m_mutex};
		while (m_bytes_pending > 0) {
			m_cond.wait(lock);
		}
	}

private:
	bool is_batch_full() const {
		return m_bytes_pending >= m_batch_size;
	}

private:
	/*
	 * Minimal amount of data being sent to a remote backends simultaneously
	 */
	const uint64_t MINIMAL_BATCH_SIZE = 1024 * 1024;
	uint64_t m_batch_size;

	uint64_t m_bytes_pending;
	uint64_t m_bytes_processed;

	/*
	 * batch size is either increased or decreased depending on
	 * whether it was processed within this timeout or not
	 */
	const int BATCH_TIMEOUT_MSEC = 1000;
	ioremap::elliptics::util::steady_timer m_batch_timer;
	bool m_need_restart_timer;

	std::mutex m_mutex;
	std::condition_variable m_cond;
};

class base_object_sender {
public:
	base_object_sender(eblob_backend_config *backend,
	                   dnet_net_state *st,
	                   const uint64_t iterator_id,
	                   dnet_cmd *cmd,
	                   const ioremap::elliptics::dnet_server_send_request &request,
	                   std::shared_ptr<iterated_key_info> info,
	                   congestion_control_monitor &monitor,
	                   std::atomic<uint64_t> &counter)
	: log_{backend->blog}
	, st_{st}
	, iterator_id_{iterator_id}
	, cmd_(cmd)
	, request_(request)
	, info_{std::move(info)}
	, monitor_(monitor)
	, counter_(counter)
	, pool_(dnet_backend_get_pool(st->n, backend->data.stat_id))
	, session_{st->n} {
		json_.reserve(request_.chunk_size);
		data_.reserve(request_.chunk_size);

		session_.set_exceptions_policy(ioremap::elliptics::session::no_exceptions);
		session_.set_filter(ioremap::elliptics::filters::all_final);
		session_.set_trace_id(cmd_->trace_id);
		session_.set_trace_bit(!!(cmd_->flags & DNET_FLAGS_TRACE_BIT));
		session_.set_groups(request_.groups);
		session_.set_user_flags(info_->ehdr.flags);
		session_.set_ioflags(DNET_IO_FLAGS_CAS_TIMESTAMP);
		session_.set_json_timestamp(info_->jhdr.timestamp);
		session_.set_timestamp(info_->ehdr.timestamp);

		chunk_timeout_ = std::max(1lu, (request_.chunk_write_timeout - 1) / 1000 + 1);
		commit_timeout_ = [&] {
			const auto size = info_->data_size + info_->jhdr.size;
			if (!size) {
				// If record is empty use just 1 second timeout
				return 1lu;
			}

			const auto number_of_chunks = (size - 1) / request_.chunk_size + 1;
			// commit operation includes last chunk write, so its timeout should be taken into account
			const auto timeout = request_.chunk_write_timeout +
				request_.chunk_commit_timeout * number_of_chunks;
			return std::max(1lu, (timeout - 1) / 1000 + 1);
		}();

		session_.set_timeout(chunk_timeout_);

		dnet_setup_id(&id_, cmd_->id.group_id, info_->key.id);

		DNET_LOG_DEBUG(log_, "EBLOB: server_send: size: {}, write_timeout: {}, commit_timeout: {}",
		               info_->data_size + info_->jhdr.size, session_.get_timeout(), commit_timeout_);
	}

	virtual ~base_object_sender() {
		unlock_quota();
		unlock_id();
	}


protected:
	void lock_id() {
		if (id_locked_)
			return;

		dnet_oplock(pool_, &id_);
		id_locked_ = true;
	}

	void unlock_id() {
		if (!id_locked_)
			return;

		dnet_opunlock(pool_, &id_);
		id_locked_ = false;
	}

	void lock_quota() {
		if (locked_quota_)
			return;

		locked_quota_ = json_.size() + data_.size();
		monitor_.add_bytes(locked_quota_);
	}

	void unlock_quota() {
		if (!locked_quota_)
			return;

		monitor_.remove_bytes(locked_quota_);
		locked_quota_ = 0;
	}

	int read() {
		lock_id();

		// read json only with first chunk of data (data_offset_ == 0)
		if (info_->jhdr.size && !data_offset_) {
			json_.resize(info_->jhdr.size, 0);
			const int err = dnet_read_ll(info_->fd, &json_.front(), json_.size(), info_->json_offset);
			if (err) {
				DNET_LOG_ERROR(log_, "EBLOB: server_send: {}: failed to read json: {}",
				               dnet_dump_id_str(info_->key.id), dnet_print_error(err));
				return err;
			}
		}

		const auto remaining_size = info_->data_size - data_offset_;
		const auto chunk_size = std::min(remaining_size, request_.chunk_size);

		data_.resize(chunk_size, 0);

		if (!data_.empty()) {
			const auto read_offset = info_->data_offset + data_offset_;
			const int err = dnet_read_ll(info_->fd, &data_.front(), data_.size(), read_offset);
			if (err) {
				DNET_LOG_ERROR(log_, "EBLOB: server_send: {}: failed to read data: {}",
				               dnet_dump_id_str(info_->key.id), dnet_print_error(err));
				return err;
			}
		}

		lock_quota();

		if (remaining_size <= request_.chunk_size) {
			// unlock id because all its data has been read
			unlock_id();
		}

		return 0;
	}

	void send_response(int status) {
		ioremap::elliptics::dnet_iterator_response response;
		response.iterator_id = iterator_id_;
		response.key = info_->key;
		response.status = status;
		response.iterated_keys = ++counter_;
		response.total_keys = request_.keys.size();
		response.record_flags = info_->record_flags;
		response.user_flags = info_->ehdr.flags;
		response.json_timestamp = info_->jhdr.timestamp;
		response.json_size = info_->jhdr.size;
		response.json_capacity = info_->jhdr.capacity;
		response.read_json_size = 0;
		response.data_timestamp = info_->ehdr.timestamp;
		response.data_size = info_->data_size;
		response.read_data_size = 0;
		response.data_offset = info_->data_offset;
		response.blob_id = static_cast<uint64_t>(info_->fd);

		auto response_packed = serialize(std::move(response));
		dnet_send_reply(st_, cmd_, response_packed.data(), response_packed.size(), 1, /*context*/ nullptr);
	}

	uint64_t remaining_size() const {
		uint64_t remaining_size = info_->data_size - data_offset_;
		if (!data_offset_) {
			// include json size that will be written with first data chunk
			remaining_size += info_->jhdr.size;
		}
		return remaining_size;
	}

	ioremap::elliptics::data_pointer json() {
		return ioremap::elliptics::data_pointer::from_raw(&json_.front(), json_.size());
	}

	ioremap::elliptics::data_pointer data() {
		return ioremap::elliptics::data_pointer::from_raw(&data_.front(), data_.size());
	}


	dnet_logger *log_;
	dnet_net_state *st_;

	const uint64_t iterator_id_;

	dnet_cmd *cmd_;
	const ioremap::elliptics::dnet_server_send_request &request_;

	const std::shared_ptr<iterated_key_info> info_;
	congestion_control_monitor &monitor_;
	uint64_t locked_quota_{0};
	std::atomic<uint64_t> &counter_;

	dnet_io_pool *pool_;
	dnet_id id_;
	bool id_locked_{false};

	ioremap::elliptics::newapi::session session_;
	uint64_t chunk_timeout_;
	uint64_t commit_timeout_;

	std::string json_;
	std::string data_;
	uint64_t data_offset_{0};
};

class small_object_sender : public base_object_sender, public std::enable_shared_from_this<small_object_sender> {
public:
	using base_object_sender::base_object_sender;

	void send() {
		retry_count_ = request_.chunk_retry_count;

		if (const int err = read()) {
			send_response(err);
			return;
		}

		write();
	}

private:
	void write() {
		session_.write(info_->key, json(), info_->jhdr.capacity, data(), info_->data_size)
			.connect(std::bind(&small_object_sender::on_write, shared_from_this(), std::placeholders::_1,
			                   std::placeholders::_2));
	}

	void on_write(const ioremap::elliptics::newapi::sync_write_result &results,
	              const ioremap::elliptics::error_info &/*error*/) {
		if (st_->__need_exit) {
			DNET_LOG_ERROR(log_, "EBLOB: Interrupting server_send: peer has been disconnected");
			return;
		}

		// collect groups with timeout
		auto retry_groups = [&] {
			std::vector<int> groups;
			groups.reserve(results.size());

			for (const auto &result: results) {
				const auto group_id = result.command()->id.group_id;
				switch (result.status()) {
					case 0: break; // skip successes
					case -ETIMEDOUT: // collect timed-out groups
						groups.emplace_back(group_id);
						break;
					default: // store one other error
						last_error_ = result.status();
						break;
				}
			}
			return groups;

		}();

		if (retry_count_-- && !retry_groups.empty()) {
			DNET_LOG_INFO(log_, "EBLOB: server_send {}: small_object_sender: retrying write to groups: {}"
			                    ", retry: {:d}/{:d}",
			              dnet_dump_id_str(info_->key.id), retry_groups,
			              request_.chunk_retry_count - retry_count_, request_.chunk_retry_count);

			session_.set_groups(retry_groups);
			write();
			return;
		}

		if (!retry_groups.empty())
			last_error_ = -ETIMEDOUT;

		send_response(last_error_);
	}

	uint8_t retry_count_{0};
	int last_error_{0};
};

class large_object_sender : public base_object_sender {
public:
	using base_object_sender::base_object_sender;

	void send() {
		active_groups_.insert(request_.groups.begin(), request_.groups.end());
		retry_groups_.reserve(request_.groups.size());

		while (read_and_write_chunk()) {
			data_offset_ += data_.size();
		}

		send_response(last_error_);
	}

private:
	bool read_and_write_chunk() {
		if (st_->__need_exit)
			return false;

		if (!remaining_size())
			return false;

		if (const int err = read()) {
			// TODO: should we remove the key from active_groups if read was failed?
			last_error_ = err;
			return false;
		}

		session_.set_groups({active_groups_.begin(), active_groups_.end()});
		auto retry_count = request_.chunk_retry_count;
		do {
			if (!retry_groups_.empty()) {
				DNET_LOG_INFO(log_, "EBLOB: server_send {}: large_object_sender: retrying write to "
				                    "groups: {}, retry: {:d}/{:d}",
				              dnet_dump_id_str(info_->key.id), retry_groups_,
				              request_.chunk_retry_count - retry_count,
				              request_.chunk_retry_count);
				session_.set_groups(retry_groups_);
				retry_groups_.clear();
			}

			for (const auto &result: write()) {
				const auto group_id = result.command()->id.group_id;
				switch (result.status()) {
					case 0: break; // skip successes
					case -ETIMEDOUT: // collect timed-out groups
						retry_groups_.emplace_back(group_id);
						break;
					default: // store one other error and remove group from active
						last_error_ = result.status();
						active_groups_.erase(group_id);
						break;
				}
			}
		} while (retry_count-- && !retry_groups_.empty());

		if (!retry_groups_.empty()) {
			// set error to -ETIMEDOUT if retry groups aren't empty and
			// error wasn't already set
			last_error_ = -ETIMEDOUT;
			// remove groups that were failed to retry from active groups
			for (auto group_id: retry_groups_) {
				active_groups_.erase(group_id);
			}
			retry_groups_.clear();
		}

		// if active_groups are empty then there is no group can continue writes,
		// so stop writing and answer to client with the last error.
		return !active_groups_.empty();
	}

	ioremap::elliptics::newapi::async_write_result write() {
		if (!data_offset_) {
			return session_.write_prepare(info_->key,
			                              json(), info_->jhdr.capacity,
			                              data(), 0, info_->data_size);
		} else if (remaining_size() > request_.chunk_size) {
			return session_.write_plain(info_->key, "", data(), data_offset_);
		} else {
			session_.set_timeout(commit_timeout_);
			return session_.write_commit(info_->key, "", data(), data_offset_, info_->data_size);
		}
	}


	std::unordered_set<int> active_groups_;
	std::vector<int> retry_groups_;
	int last_error_{0};
};

static iterator_callback make_iterator_server_send_callback(eblob_backend_config *c,
                                                            dnet_net_state *st,
                                                            dnet_cmd *cmd,
                                                            const ioremap::elliptics::dnet_server_send_request &request,
                                                            uint64_t iterator_id,
                                                            congestion_control_monitor &monitor,
                                                            std::atomic<uint64_t> &counter) {
	using namespace ioremap::elliptics;
	return [=, &request, &counter, &monitor] (std::shared_ptr<iterated_key_info> info) -> int {
		if (st->__need_exit) {
			DNET_LOG_ERROR(c->blog, "EBLOB: Interrupting server_send: peer has been disconnected");
			return -EINTR;
		}

		// use small key sender if data_size is less than chunk_size
		if (info->data_size <= request.chunk_size) {
			const auto sender = std::make_shared<small_object_sender>(c, st, iterator_id, cmd, request,
			                                                          info, monitor, counter);
			sender->send();
		} else {
			large_object_sender sender{c, st, iterator_id, cmd, request, info, monitor, counter};
			sender.send();
		}

		return 0;
	};
}

static iterator_callback make_iterator_network_callback(eblob_backend_config *c, dnet_net_state *st,
                                                        dnet_cmd *cmd,
                                                        ioremap::elliptics::dnet_iterator_request &request,
                                                        const dnet_iterator *it) {
	using namespace ioremap::elliptics;
	auto counter = std::make_shared<std::atomic<uint64_t>>(0);
	const uint64_t total_keys = eblob_total_elements(c->eblob);

	return [=, &request] (std::shared_ptr<iterated_key_info> info) -> int {
		if (st->__need_exit) {
			DNET_LOG_ERROR(c->blog, "EBLOB: iterator: Interrupting iterator: peer has been disconnected");
			return -EINTR;
		}

		data_pointer json;
		if ((request.flags & DNET_IFLAGS_JSON) && info->jhdr.size) {
			json = data_pointer::allocate(info->jhdr.size);
			const int err = dnet_read_ll(info->fd, json.data<char>(), json.size(), info->json_offset);
			if (err) {
				DNET_LOG_ERROR(c->blog, "EBLOB: iterator: {}: failed to read json: {} [{}]",
				               dnet_dump_id_str(info->key.id), strerror(-err), err);
				return err;
			}
		}

		const uint64_t read_data_size = (request.flags & DNET_IFLAGS_DATA) ? info->data_size : 0;

		ioremap::elliptics::dnet_iterator_response response;
		response.iterator_id = it->id;
		response.key = info->key;
		response.status = 0;
		response.iterated_keys = ++(*counter);
		response.total_keys = total_keys;
		response.record_flags = info->record_flags;
		response.user_flags = info->ehdr.flags;
		response.json_timestamp = info->jhdr.timestamp;
		response.json_size = info->jhdr.size;
		response.json_capacity = info->jhdr.capacity;
		response.read_json_size = json.size();
		response.data_timestamp = info->ehdr.timestamp;
		response.data_size = info->data_size;
		response.read_data_size = read_data_size;
		response.data_offset = info->data_offset;
		response.blob_id = static_cast<uint64_t>(info->fd);

		auto header = serialize(std::move(response));

		if (st->__need_exit) {
			DNET_LOG_ERROR(c->blog,
			               "EBLOB: iterator: Interrupting iterator because peer has been disconnected");
			return -EINTR;
		}

		auto response_packed = data_pointer::allocate(sizeof(*cmd) + header.size() + json.size());

		memcpy(response_packed.data(), cmd, sizeof(*cmd));
		memcpy(response_packed.skip<dnet_cmd>().data(), header.data(), header.size());
		if (!json.empty()) {
			memcpy(response_packed.skip(sizeof(*cmd) + header.size()).data(), json.data(), json.size());
		}

		response_packed.data<dnet_cmd>()->size = header.size() + json.size() + read_data_size;
		response_packed.data<dnet_cmd>()->flags |= DNET_FLAGS_REPLY | DNET_FLAGS_MORE;
		response_packed.data<dnet_cmd>()->flags &= ~DNET_FLAGS_NEED_ACK;

		return dnet_send_fd_threshold(st, response_packed.data(), response_packed.size(),
			                      info->fd, info->data_offset, read_data_size);
	};
}


static int blob_iterate_callback_common(const eblob_backend_config *c,
                                        const ioremap::elliptics::dnet_iterator_request &request,
                                        dnet_iterator *it,
                                        const eblob_disk_control *dc, int fd, uint64_t offset,
                                        iterator_callback callback) {
	assert(dc != nullptr);

	auto info = std::make_shared<iterated_key_info>(dc, fd);

	// dc->data_size for uncommitted records contains garbage and shouldn't be used
	uint64_t size = (dc->flags & BLOB_DISK_CTL_UNCOMMITTED) ? 0 : dc->data_size;

	int err = 0;
	if (dc->flags & BLOB_DISK_CTL_EXTHDR) {
		if (!(request.flags & DNET_IFLAGS_NO_META)) {
			err = dnet_ext_hdr_read(&info->ehdr, fd, offset);
			if (err) {
				DNET_LOG_ERROR(c->blog, "EBLOB: iterator: {}: dnet_ext_hdr_read failed: {} [{}]",
				               dnet_dump_id_str(info->key.id), strerror(-err), err);
				return err;
			}

			if (info->ehdr.size) {
				err = dnet_read_json_header(fd, offset + sizeof(info->ehdr), info->ehdr.size, &info->jhdr);
				if (err) {
					DNET_LOG_ERROR(c->blog,
					               "EBLOB: iterator: {}: dnet_read_json_header failed: {} [{}]",
					               dnet_dump_id_str(info->key.id), strerror(-err), err);
					return err;
				}
			}
		}

		offset += sizeof(info->ehdr) + info->ehdr.size;
		if (size >= sizeof(info->ehdr) + info->ehdr.size) {
			size -= sizeof(info->ehdr) + info->ehdr.size;
		} else if (size) {
			err = -EINVAL;
			DNET_LOG_ERROR(c->blog, "EBLOB: iterator: {}: has invalid size: {} < {} (sizeof(info.ehdr)) + "
			                        "{} (info.ehdr.size): {} [{}]",
			               dnet_dump_id_str(info->key.id), size, sizeof(info->ehdr), info->ehdr.size,
			               strerror(-err), err);
			return err;
		}
	}

	if (request.flags & DNET_IFLAGS_TS_RANGE) {
		if (dnet_time_cmp(&info->ehdr.timestamp, &std::get<0>(request.time_range)) < 0 ||
		    dnet_time_cmp(&info->ehdr.timestamp, &std::get<1>(request.time_range)) > 0) {
			/* skip key which timestamp is not in request.time_range */
			return 0;
		}
	}

	if (size >= info->jhdr.capacity) {
		info->data_size = size - info->jhdr.capacity;
	} else if (size) {
		err = -EINVAL;
		DNET_LOG_ERROR(c->blog, "EBLOB: iterator {}: has invalid size({}) < info.jhdr.capacity({}): {} [{}]",
		               dnet_dump_id_str(info->key.id), size, info->jhdr.capacity, strerror(-err), err);
		return err;
	}

	info->json_offset = offset;
	info->data_offset = offset + info->jhdr.capacity;

	const std::string data_ts = dnet_print_time(&info->ehdr.timestamp);
	const std::string json_ts = dnet_print_time(&info->jhdr.timestamp);

	DNET_LOG_DEBUG(c->blog, "EBLOB: iterated: key: {}, fd: {}, user_flags: {:#x}, json: {{offset: {}, size: {}, "
	                        "capacity: {}, ts: {}}}, data: {{offset: {}, size: {}, ts: {}}}",
	               dnet_dump_id_str(info->key.id), fd, info->ehdr.flags, offset, info->jhdr.size,
	               info->jhdr.capacity, json_ts, info->data_offset, info->data_size, data_ts);

	err = callback(info);
	if (err) {
		return err;
	}

	return dnet_iterator_flow_control(it);
}

static int blob_iterator_start(struct eblob_backend_config *c, dnet_net_state *st, dnet_cmd *cmd,
                               ioremap::elliptics::dnet_iterator_request &request) {
	using namespace ioremap::elliptics;

	if (request.flags & ~DNET_IFLAGS_ALL) {
		DNET_LOG_ERROR(c->blog, "EBLOB: iteration failed: unknown iteration flags: {}", request.flags);
		return -ENOTSUP;
	}

	if (request.type <= DNET_ITYPE_FIRST ||
	    request.type >= DNET_ITYPE_LAST) {
		DNET_LOG_ERROR(c->blog, "EBLOB: iteration failed: unknown iteration type: {}", request.type);
		return -ENOTSUP;
	}

	if (!check_key_ranges(c, request)) {
		return -ERANGE;
	}

	if (!check_ts_range(c, request)) {
		return -ERANGE;
	}

	eblob_iterate_control control;
	memset(&control, 0, sizeof(control));

	control.b = c->eblob;
	control.log = c->data.log;
	control.flags = EBLOB_ITERATE_FLAGS_ALL | EBLOB_ITERATE_FLAGS_READONLY;

	std::vector<eblob_index_block> ranges;
	ranges.reserve(request.key_ranges.size());

	for (const auto &range : request.key_ranges) {
		eblob_key begin, end;
		memcpy(begin.id, range.key_begin.id, EBLOB_ID_SIZE);
		memcpy(end.id, range.key_end.id, EBLOB_ID_SIZE);

		ranges.emplace_back(eblob_index_block{begin, end, 0, 0});
	}

	control.range = ranges.data();
	control.range_num = ranges.size();

	auto deleter = [&st] (dnet_iterator *p) {
		dnet_iterator_destroy(st->n, p);
	};

	std::unique_ptr<dnet_iterator, decltype(deleter)> it{dnet_iterator_create(st->n), deleter};
	if (!it) {
		return -ENOMEM;
	}

	iterator_callback callback;

	switch (request.type) {
		case DNET_ITYPE_DISK: {
			DNET_LOG_ERROR(c->blog, "EBLOB: iteration failed: type: 'DNET_ITYPE_DISK' is not implemented");
			return -ENOTSUP;
		}
		case DNET_ITYPE_NETWORK: {
			callback = make_iterator_network_callback(c, st, cmd, request, it.get());
			break;
		}
		default: {
		        DNET_LOG_ERROR(c->blog, "EBLOB: iteration failed: unknown type: {}", request.type);
		        return -ENOTSUP;
		}
	}

	auto common_callback = [&] (const eblob_disk_control *dc, int fd, uint64_t data_offset) -> int {
		return blob_iterate_callback_common(c, request, it.get(), dc, fd, data_offset, callback);
	};

	control.priv = &common_callback;

	control.iterator_cb.iterator = [] (eblob_disk_control *dc, eblob_ram_control *,
	                                   int fd, uint64_t data_offset, void *priv, void *) ->int {
		auto callback = *static_cast<decltype(common_callback) *>(priv);
		return callback(dc, fd, data_offset);
	};

	return eblob_iterate(c->eblob, &control);
}

int blob_iterate(struct eblob_backend_config *c,
                 void *state,
                 struct dnet_cmd *cmd,
                 void *data,
                 dnet_access_context *context) {
	using namespace ioremap::elliptics;
	/*
	 * Sanity
	 */
	if (state == nullptr || cmd == nullptr || data == nullptr) {
		return -EINVAL;
	}

	ioremap::elliptics::dnet_iterator_request request;
	deserialize(data_pointer::from_raw(data, cmd->size), request);

	if (context) {
		auto groups = [&] {
			std::ostringstream groups;
			groups << request.groups;
			return groups.str();
		}();

		context->add({{"iterator_id", request.iterator_id},
		              {"action", request.action},
		              {"type", request.type},
		              {"flags", to_hex_string(request.flags)},
		              {"key_ranges", request.key_ranges.size()},
		              {"groups", groups},
		             });
	}

	DNET_LOG_INFO(c->blog, "EBLOB: {} started: id: {}, flags: {}, action: {}, type: {}, key_ranges: {}, groups: {}",
	              __func__, request.iterator_id, request.flags, request.action, request.type,
	              request.key_ranges.size(), request.groups.size());

	/*
	 * Check iterator action start/pause/cont
	 * On pause, find in list and mark as stopped
	 * On continue, find in list and mark as running, broadcast condition variable.
	 * On start, create and start iterator.
	 */
	int err = 0;
	switch (request.action) {
		case DNET_ITERATOR_ACTION_START:
			err = blob_iterator_start(c, reinterpret_cast<dnet_net_state*>(state), cmd, request);
			break;
		case DNET_ITERATOR_ACTION_PAUSE:
		case DNET_ITERATOR_ACTION_CONTINUE:
		case DNET_ITERATOR_ACTION_CANCEL:
			err = -ENOTSUP;
			break;
		default:
			err = -ENOTSUP;
			break;
	}

	DNET_LOG(c->blog, err ? DNET_LOG_ERROR : DNET_LOG_INFO, "EBLOB: {} finished: {} [{}]", __func__, strerror(-err),
	         err);

	return err;
}

int blob_send_new(struct eblob_backend_config *c,
                  void *state,
                  struct dnet_cmd *cmd,
                  void *data,
                  dnet_access_context *context) {
	using namespace ioremap::elliptics;

	if (c == nullptr || state == nullptr || cmd == nullptr || data == nullptr)
		return -EINVAL;

	ioremap::elliptics::dnet_server_send_request request;
	deserialize(data_pointer::from_raw(data, cmd->size), request);

	auto groups = [&] {
		std::ostringstream groups;
		groups << request.groups;
		return groups.str();
	}();

	if (context) {
		context->add({{"keys", request.keys.size()},
		              {"backend_id", c->data.stat_id},
		              {"chunk_size", request.chunk_size},
		              {"flags", to_hex_string(request.flags)},
		              {"groups", groups},
		              {"chunk_write_timeout", request.chunk_write_timeout},
		              {"chunk_commit_timeout", request.chunk_commit_timeout},
		              {"chunk_retry_count", request.chunk_retry_count},
		             });
	}

	DNET_LOG_INFO(c->blog, "EBLOB: {} started: ids_num: {}, groups: {}, chunk_write_timeout: {}, "
	                       "chunk_commit_timeout: {}, chunk_retry_count: {:d}",
	              __func__, request.keys.size(), request.groups, request.chunk_write_timeout,
	              request.chunk_commit_timeout, request.chunk_retry_count);

	int err = 0;

	std::atomic<uint64_t> counter(0);
	ioremap::elliptics::dnet_iterator_response response;
	response.iterator_id = uint64_t(cmd->backend_id);
	response.key = dnet_raw_id{{0}};
	response.status = 0;
	response.iterated_keys = 0;
	response.total_keys = request.keys.size();
	response.record_flags = 0;
	response.user_flags = 0;
	response.json_timestamp = dnet_time{0, 0};
	response.json_size = 0;
	response.json_capacity = 0;
	response.read_json_size = 0;
	response.data_timestamp = dnet_time{0, 0};
	response.data_size = 0;
	response.read_data_size = 0;
	response.data_offset = 0;
	response.blob_id = 0;

	auto send_fail_reply = [&] (int status) {
		response.status = status;
		response.iterated_keys = ++counter;

		auto response_data = serialize(response);
		return dnet_send_reply(state, cmd, response_data.data(), response_data.size(), 1, /*context*/ nullptr);
	};

	congestion_control_monitor monitor;

	auto callback = make_iterator_server_send_callback(c, reinterpret_cast<dnet_net_state*>(state),
	                                                   cmd, request, cmd->backend_id, monitor, counter);

	eblob_key ekey;
	eblob_write_control wc;
	uint64_t size = 0, offset = 0;

	for (const auto &key: request.keys) {
		response.key = key;

		memcpy(ekey.id, key.id, EBLOB_ID_SIZE);

		err = blob_read_and_check_flags_new(c, &ekey, &wc);
		if (err) {
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob_send_new: lookup failed: {}", dnet_dump_id_str(key.id),
			               dnet_print_error(err));
			if ((err = send_fail_reply(err))) {
				break;
			}
			continue;
		}

		auto info = std::make_shared<iterated_key_info>(key, wc.flags, wc.data_fd);

		size = wc.total_data_size;
		offset = wc.data_offset;

		if (wc.flags & BLOB_DISK_CTL_EXTHDR) {
			err = dnet_ext_hdr_read(&info->ehdr, info->fd, offset);
			if (err) {
				DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob_send_new: failed to read ext header: {} [{}]",
				               dnet_dump_id_str(key.id), strerror(-err), err);
				if ((err = send_fail_reply(err))) {
					break;
				}
				continue;
			}

			offset += sizeof(info->ehdr);

			if (size >= sizeof(info->ehdr)) {
				size -= sizeof(info->ehdr);
			} else if (size) {
				err = -ERANGE;
				DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob_send_new: invalid record: total_data_size({}) "
				                        "< ehdr({}): {} [{}]",
				               dnet_dump_id_str(key.id), size, sizeof(info->ehdr), strerror(-err), err);
				if ((err = send_fail_reply(err))) {
					break;
				}
				continue;
			}

			if (info->ehdr.size) {
				err = dnet_read_json_header(info->fd, offset, info->ehdr.size, &info->jhdr);
				if (err) {
					DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob_send_new: failed to read json header: {} [{}]",
					               dnet_dump_id_str(key.id), strerror(-err), err);
					if ((err = send_fail_reply(err))) {
						break;
					}
					continue;
				}
			}

			offset += info->ehdr.size;

			if (size >= info->ehdr.size) {
				size -= info->ehdr.size;
			} else if (size) {
				err = -ERANGE;
				DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob_send_new: invalid record: total_data_size({}) "
				                        "< ehdr({}) + json header({}) = {}: {} [{}]",
				               dnet_dump_id_str(key.id), wc.total_data_size, sizeof(info->ehdr),
				               info->ehdr.size, sizeof(info->ehdr) + info->ehdr.size, strerror(-err),
				               err);
				if ((err = send_fail_reply(err))) {
					break;
				}
				continue;
			}
		}

		if (size >= info->jhdr.capacity) {
			info->data_size = size - info->jhdr.capacity;
		} else if (size) {
			err = -ERANGE;
			DNET_LOG_ERROR(c->blog, "{}: EBLOB: blob_send_new: invalid record: total_data_size({}) "
			                        "< ehdr({}) + json header({}) + json capacity({}) = {}: {} [{}]",
			               dnet_dump_id_str(key.id), wc.total_data_size, sizeof(info->ehdr),
			               info->jhdr.capacity, info->ehdr.size,
			               sizeof(info->ehdr) + info->ehdr.size + info->jhdr.capacity, strerror(-err), err);
			if ((err = send_fail_reply(err))) {
				break;
			}
			continue;
		}

		info->json_offset = offset;
		info->data_offset = offset + info->jhdr.capacity;

		wc.offset = 0;
		wc.size = sizeof(info->ehdr) + info->ehdr.size + info->jhdr.capacity + info->data_size;

		err = blob_read_and_check_stamp(c, &info->ehdr.timestamp, info->fd, info->data_offset, info->data_size);
		if (err) {
			if ((err = send_fail_reply(err))) {
				break;
			}
			continue;
		}

		err = eblob_verify_checksum(c->eblob, &ekey, &wc);
		if (err) {
			if ((err = send_fail_reply(err))) {
				break;
			}
			continue;
		}

		if ((err = callback(info))) {
			break;
		}
	}

	monitor.wait_completion();

	DNET_LOG(c->blog, err ? DNET_LOG_ERROR : DNET_LOG_INFO, "EBLOB: {} finished: {}", __func__,
	         dnet_print_error(err));
	return err;
}

int blob_bulk_read_new(struct eblob_backend_config *c,
                       void *state,
                       struct dnet_cmd *cmd,
                       void *data,
		       struct dnet_cmd_stats *cmd_stats,
		       dnet_access_context *context) {
	using namespace ioremap::elliptics;

	if (c == nullptr || state == nullptr || cmd == nullptr || data == nullptr)
		return -EINVAL;

	dnet_bulk_read_request bulk_request;
	deserialize(data_pointer::from_raw(data, cmd->size), bulk_request);

	auto st = reinterpret_cast<dnet_net_state *>(state);
	const int backend_id = c->data.stat_id;

	if (context) {
		context->add({{"keys", bulk_request.keys.size()},
		              {"ioflags", std::string(dnet_flags_dump_ioflags(bulk_request.ioflags))},
		              {"read_flags", std::string(dnet_dump_read_flags(bulk_request.read_flags))},
		              {"backend_id", backend_id},
		             });
	}

	auto backend = st->n->io->backends_manager->get(backend_id);
	if (!backend)
		return -ENOTSUP;

	auto pool = backend->io_pool();
	if (!pool) {
		DNET_LOG_ERROR(c->blog, "EBLOB: {}: couldn't find pool for backend_id: {}",
			       __func__, backend_id);
		return -EINVAL;
	}

	int err = 0;
	dnet_read_request request;
	request.ioflags = bulk_request.ioflags;
	request.read_flags = bulk_request.read_flags;
	request.data_offset = request.data_size = 0;
	request.deadline = bulk_request.deadline;

	ioremap::elliptics::util::steady_timer timer;
	struct dnet_cmd_stats orig_stats(*cmd_stats);

	cmd->flags &= ~DNET_FLAGS_NEED_ACK;
	struct dnet_cmd cmd_copy(*cmd);
	const auto num_keys = bulk_request.keys.size();
	for (size_t i = 0; i < num_keys && !st->__need_exit; ++i) {
		timer.restart();

		cmd_copy.status = 0;
		cmd_copy.id = bulk_request.keys[i];

		auto read_stats = orig_stats;

		const bool last_read = i >= (num_keys - 1);
		{
			dnet_oplock_guard oplock_guard{pool, &cmd_copy.id};
			// bulk_read doesn't provide its context to read to decrease verbosity
			err = blob_read_new_impl(c,
			                         state,
			                         &cmd_copy,
			                         &read_stats,
			                         request,
			                         last_read,
			                         /*context*/ nullptr);
		}
		if (err) {
			cmd_copy.status = err;
			dnet_send_reply(st, &cmd_copy, nullptr, 0, last_read ? 0 : 1, /*context*/ nullptr);
		}
		cmd_stats->size += read_stats.size;

		read_stats.handle_time = timer.get_us();

		backend->command_stats().command_counter(DNET_CMD_READ_NEW, cmd_copy.trans, err, /*handled_in_cache*/ 0,
		                                         read_stats.size, read_stats.handle_time);
	}

	return 0;
}

int blob_bulk_remove_new(struct eblob_backend_config *config,
                         void *state,
                         struct dnet_cmd *cmd,
                         void *data,
                         struct dnet_access_context *context) {
	using namespace ioremap::elliptics;

	if (config == nullptr || state == nullptr || cmd == nullptr || data == nullptr)
		return -EINVAL;

	dnet_bulk_remove_request bulk_request;
	deserialize(data_pointer::from_raw(data, cmd->size), bulk_request);
	if (!bulk_request.is_valid())
		return -EINVAL;

	auto st = reinterpret_cast<dnet_net_state *>(state);
	const int backend_id = config->data.stat_id;
	auto backend = st->n->io->backends_manager->get(backend_id);
	if (!backend)
		return -ENOTSUP;

	if (context) {
		context->add({{"keys", bulk_request.keys.size()},
			      {"ioflags", std::string(dnet_flags_dump_ioflags(bulk_request.ioflags))},
			      {"backend_id", backend_id},
			     });
	}
	
	auto pool = backend->io_pool();
	if (!pool) {
		DNET_LOG_ERROR(config->blog, "EBLOB: {}: couldn't find pool for backend_id: {}",
		               __func__, backend_id);
		return -EINVAL;
	}

	DNET_LOG_INFO(config->blog, "{}: EBLOB: {}: BULK_REMOVE_NEW: start for backend_id: {} ",
	              dnet_dump_id(&cmd->id), __func__, backend_id);

	eblob_backend *b = config->eblob;

	cmd->flags &= ~DNET_FLAGS_NEED_ACK;

	const size_t num_keys = bulk_request.keys.size();
	for (size_t i = 0; i < num_keys; ++i) {
		int err = 0;
		struct dnet_cmd cmd_copy(*cmd);
		cmd_copy.backend_id = backend_id;
		cmd_copy.id = bulk_request.keys[i];

		struct eblob_key key;
		memcpy(key.id, bulk_request.keys[i].id, EBLOB_ID_SIZE);
		
		{
			dnet_oplock_guard oplock_guard{pool, &cmd_copy.id};
			if (bulk_request.ioflags & DNET_IO_FLAGS_CAS_TIMESTAMP) {
				dnet_remove_request single_request;
				single_request.ioflags = bulk_request.ioflags;
				single_request.timestamp = bulk_request.timestamps[i];
				err = blob_del_new_cas(config, b, &cmd_copy, key, single_request);
			}
			if (!err) {
				err = eblob_remove(b, &key);
			}
		}

		cmd_copy.status = err;
		const bool last_read = i >= (num_keys - 1);
		dnet_send_reply(st, &cmd_copy, nullptr, 0, last_read ? 0 : 1, /*context*/ nullptr);
	}
	return 0;
}
