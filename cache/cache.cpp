/*
* 2012+ Copyright (c) Evgeniy Polyakov <zbr@ioremap.net>
* 2013+ Copyright (c) Ruslan Nigmatullin <euroelessar@yandex.ru>
* 2013+ Copyright (c) Andrey Kashin <kashin.andrej@gmail.com>
* All rights reserved.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*/

#include "cache.hpp"
#include "slru_cache.hpp"

#include <blackhole/attribute.hpp>
#include <kora/config.hpp>

#include "library/access_context.h"
#include "library/backend.h"
#include "library/msgpack_conv.hpp"
#include "library/protocol.hpp"

#include "monitor/measure_points.h"
#include "rapidjson/document.h"

#include "example/config.hpp"
#include "local_session.h"

namespace ell = ioremap::elliptics;

namespace ioremap { namespace cache {

static size_t parse_size(const std::string &value) {
	size_t ret = strtoul(value.c_str(), NULL, 0);

	if (strchr(value.c_str(), 'P') || strchr(value.c_str(), 'p')) {
		ret *= 1ULL << 50;
	} else if (strchr(value.c_str(), 'T') || strchr(value.c_str(), 't')) {
		ret *= 1ULL << 40;
	} else if (strchr(value.c_str(), 'G') || strchr(value.c_str(), 'g')) {
		ret *= 1ULL << 30;
	} else if (strchr(value.c_str(), 'M') || strchr(value.c_str(), 'm')) {
		ret *= 1ULL << 20;
	} else if (strchr(value.c_str(), 'K') || strchr(value.c_str(), 'k')) {
		ret *= 1ULL << 10;
	}

	return ret;
}

static size_t parse_size(const kora::config_t &value) {
	size_t ret = 0;
	if (value.underlying_object().is_uint()) {
		ret = value.to<size_t>();
	} else if (value.underlying_object().is_string()) {
		ret = parse_size(value.to<std::string>());
	} else {
		throw elliptics::config::config_error(value.path() + " must be specified");
	}

	if (ret == 0) {
		throw elliptics::config::config_error(value.path() + " must be non-zero");
	}
	return ret;
}

cache_config cache_config::parse(const kora::config_t &cache) {
	return {/*size*/ parse_size(cache["size"]),
	        /*count*/ cache.at<size_t>("shards", DNET_DEFAULT_CACHES_NUMBER),
	        /*sync_timeout*/ cache.at<unsigned>("sync_timeout", DNET_DEFAULT_CACHE_SYNC_TIMEOUT_SEC),
	        /*pages_proportions*/ cache.at("pages_proportions",
	                                       std::vector<size_t>(DNET_DEFAULT_CACHE_PAGES_NUMBER, 1))};
}

cache_manager::cache_manager(dnet_node *n, dnet_backend &backend, const cache_config &config)
: m_node(n)
, m_need_exit(false) {
	size_t caches_number = config.count;
	m_cache_pages_number = config.pages_proportions.size();
	m_max_cache_size = config.size;
	size_t max_size = m_max_cache_size / caches_number;

	size_t proportionsSum = 0;
	for (size_t i = 0; i < m_cache_pages_number; ++i) {
		proportionsSum += config.pages_proportions[i];
	}

	std::vector<size_t> pages_max_sizes(m_cache_pages_number);
	for (size_t i = 0; i < m_cache_pages_number; ++i) {
		pages_max_sizes[i] = max_size * (config.pages_proportions[i] * 1.0 / proportionsSum);
	}

	for (size_t i = 0; i < caches_number; ++i) {
		m_caches.emplace_back(
		        std::make_shared<slru_cache_t>(n, backend, pages_max_sizes, config.sync_timeout, m_need_exit));
	}
}

cache_manager::~cache_manager() {
	m_need_exit = true;
}

write_response_t cache_manager::write(dnet_net_state *st,
                                      ell::dnet_write_request *request,
                                      dnet_access_context *context) {
	return m_caches[idx(request->cmd.id.id)]->write(st, request, context);
}

read_response_t cache_manager::read(const unsigned char *id, uint64_t ioflags) {
	return m_caches[idx(id)]->read(id, ioflags);
}

int cache_manager::remove(ell::dnet_remove_request *request,
                          dnet_access_context *context) {
	return m_caches[idx(request->cmd.id.id)]->remove(request, context);
}

read_response_t cache_manager::lookup(const unsigned char *id) {
	return m_caches[idx(id)]->lookup(id);
}

void cache_manager::clear() {
	for (size_t i = 0; i < m_caches.size(); ++i) {
		m_caches[i]->clear();
	}
}

size_t cache_manager::cache_size() const {
	return m_max_cache_size;
}

size_t cache_manager::cache_pages_number() const {
	return m_cache_pages_number;
}

cache_stats cache_manager::get_total_cache_stats() const {
	cache_stats stats;
	stats.pages_sizes.resize(m_cache_pages_number);
	stats.pages_max_sizes.resize(m_cache_pages_number);
	for (size_t i = 0; i < m_caches.size(); ++i) {
		const cache_stats &page_stats = m_caches[i]->get_cache_stats();
		stats.number_of_objects += page_stats.number_of_objects;
		stats.number_of_objects_marked_for_deletion += page_stats.number_of_objects_marked_for_deletion;
		stats.size_of_objects_marked_for_deletion += page_stats.size_of_objects_marked_for_deletion;
		stats.size_of_objects += page_stats.size_of_objects;

		for (size_t j = 0; j < m_cache_pages_number; ++j) {
			stats.pages_sizes[j] += page_stats.pages_sizes[j];
			stats.pages_max_sizes[j] += page_stats.pages_max_sizes[j];
		}
	}
	return stats;
}

void cache_manager::statistics(rapidjson::Value &value, rapidjson::Document::AllocatorType &allocator) const {
	value.SetObject();

	rapidjson::Value total_cache(rapidjson::kObjectType);
	{
		rapidjson::Value size_stats(rapidjson::kObjectType);
		get_total_cache_stats().to_json(size_stats, allocator);
		total_cache.AddMember("size_stats", size_stats, allocator);
	}
	value.AddMember("total_cache", total_cache, allocator);

	rapidjson::Value caches(rapidjson::kObjectType);
	for (size_t i = 0; i < m_caches.size(); ++i) {
		const auto &index = std::to_string(i);
		rapidjson::Value cache_time_stats(rapidjson::kObjectType);
		m_caches[i]->get_cache_stats().to_json(cache_time_stats, allocator);
		caches.AddMember(index.c_str(), allocator, cache_time_stats, allocator);
	}
	value.AddMember("caches", caches, allocator);
}

size_t cache_manager::idx(const unsigned char *id) {
	size_t i = *(size_t *)id;
	size_t j = *(size_t *)(id + DNET_ID_SIZE - sizeof(size_t));
	return (i ^ j) % m_caches.size();
}

}} /* namespace ioremap::cache */

using namespace ioremap::cache;

static int dnet_cmd_cache_io_write_new(struct cache_manager *cache,
                                       struct dnet_net_state *st,
                                       ell::dnet_write_request *request,
                                       struct dnet_cmd_stats *cmd_stats,
                                       dnet_access_context *context) {
	using namespace ell;

	if (request->ioflags & DNET_IO_FLAGS_NOCACHE) {
		return -ENOTSUP;
	}

	write_status status;
	int err;
	cache_item it;

	std::tie(status, err, it) = cache->write(st, request, context);

	if (status == write_status::HANDLED_IN_CACHE) {
		auto response = new dnet_lookup_response;

		response->cmd = request->cmd;
		response->cmd.flags |= DNET_FLAGS_REPLY;
		response->cmd.flags &= ~DNET_FLAGS_NEED_ACK;

		response->record_flags = 0;
		response->user_flags = it.user_flags;
		response->json_timestamp = it.json_timestamp;
		response->json_offset = 0;
		response->json_size = it.json->size();
		response->json_capacity = it.json->size();
		response->data_timestamp = it.timestamp;
		response->data_offset = 0;
		response->data_size = it.data->size();

		cmd_stats->size = request->json.size() + request->data.size();
		cmd_stats->handled_in_cache = 1;

		request->cmd.flags &= ~DNET_FLAGS_NEED_ACK;

		err = dnet_send_response(st, response, context);

	} else if (status == write_status::HANDLED_IN_BACKEND) {
		cmd_stats->size = request->json_size + request->data_size;
		cmd_stats->handled_in_cache = 0;

		request->cmd.flags &= ~DNET_FLAGS_NEED_ACK;
	}

	return err;
}

static int dnet_cmd_cache_io_read_new(struct cache_manager *cache,
                                      struct dnet_net_state *st,
                                      ell::dnet_read_request *request,
                                      struct dnet_cmd_stats *cmd_stats,
                                      dnet_access_context *context) {
	using namespace ell;

	if (request->ioflags & DNET_IO_FLAGS_NOCACHE) {
		return -ENOTSUP;
	}

	int err;
	cache_item it;

	std::tie(err, it) = cache->read(request->cmd.id.id, request->ioflags);
	if (err) {
		return err;
	}

	auto raw_data = it.data;
	auto raw_json = it.json;

	data_pointer json, data_p;

	if (request->read_flags & DNET_READ_FLAGS_JSON) {
		json = data_pointer::from_raw(*raw_json);
	}

	if (request->read_flags & DNET_READ_FLAGS_DATA) {
		if (request->data_offset && request->data_offset >= raw_data->size())
			return -E2BIG;

		uint64_t data_size = raw_data->size() - request->data_offset;

		if (request->data_size) {
			data_size = std::min(data_size, request->data_size);
		}

		data_p = data_pointer::from_raw(*raw_data);
		data_p = data_p.slice(request->data_offset, data_size);
	}

	auto response = new dnet_read_response;

	response->cmd = request->cmd;
	response->cmd.flags |= DNET_FLAGS_REPLY;
	response->cmd.flags &= ~DNET_FLAGS_NEED_ACK;

	response->record_flags = 0;
	response->user_flags = it.user_flags;
	response->json_timestamp = it.json_timestamp;
	response->json_size = raw_json->size();
	response->json_capacity = raw_json->size();
	response->read_json_size = json.size();
	// TODO(sabramkin): lifetime of json in cache memory = ? maybe not copy?
	response->json = data_pointer::copy(json);
	response->data_timestamp = it.timestamp;
	response->data_size = raw_data->size();
	response->read_data_offset = request->data_offset;
	response->read_data_size = data_p.size();
	// TODO(sabramkin): lifetime of data in cache memory = ? maybe not copy?
	response->data = data_place::from_memory(data_pointer::copy(data_p));

	cmd_stats->size = json.size() + data_p.size();
	cmd_stats->handled_in_cache = 1;

	request->cmd.flags &= ~DNET_FLAGS_NEED_ACK;

	return dnet_send_response(st, response, context);
}

// TODO(sabramkin): repair
//static int dnet_cmd_cache_io_lookup_new(struct cache_manager *cache,
//                                        struct dnet_net_state *st,
//                                        struct dnet_cmd *cmd,
//                                        struct dnet_cmd_stats *cmd_stats,
//                                        dnet_access_context *context) {
//	using namespace ell;
//	cmd_stats->handled_in_cache = 1;
//
//	int err;
//	cache_item it;
//
//	std::tie(err, it) = cache->lookup(cmd->id.id);
//	if (err) {
//		return err;
//	}
//
//	std::vector<unsigned char> json_checksum;
//	std::vector<unsigned char> data_checksum;
//
//	if (cmd->flags & DNET_FLAGS_CHECKSUM) {
//		auto calculate_checksum = [st, cmd, context] (const std::shared_ptr<std::string> &data,
//		                                              std::vector<unsigned char> &checksum,
//		                                              const char *csum_subject,
//		                                              const char *csum_time_ctx_attr) {
//			if (!data || !data->size()) {
//				return 0;
//			}
//
//			checksum.resize(DNET_CSUM_SIZE);
//
//			util::steady_timer timer;
//			int err = dnet_checksum_data(st->n, data->data(), data->size(),
//			                             checksum.data(), checksum.size());
//			uint64_t csum_time = timer.get_us();
//			if (context) {
//				context->add({{csum_time_ctx_attr, csum_time}});
//			}
//			if (err) {
//				DNET_LOG_ERROR(st->n, "{}: {} dnet_cmd_cache_io_lookup_new: failed to calculate {} "
//				               "checksum: {} [{}]", dnet_dump_id(&cmd->id), dnet_cmd_string(cmd->cmd),
//				               csum_subject, strerror(-err), err);
//				return err;
//			}
//
//			return 0;
//		};
//
//		err = calculate_checksum(it.json, json_checksum, "json", "json_csum_time");
//		if (err) {
//			return err;
//		}
//
//		err = calculate_checksum(it.data, data_checksum, "data", "data_csum_time");
//		if (err) {
//			return err;
//		}
//	}
//
//	auto response = serialize(dnet_lookup_response{
//		0, // record_flags
//		it.user_flags, // user_flags
//		"", // path
//
//		it.json_timestamp, // json_timestamp
//		0, // json_offset
//		it.json->size(), // json_size
//		it.json->size(), // json_capacity
//		std::move(json_checksum), // json_checksum
//
//		it.timestamp, // data_timestamp
//		0, // data_offset
//		it.data->size(), // data_size
//		std::move(data_checksum), // data_checksum
//	});
//
//	cmd->flags &= ~DNET_FLAGS_NEED_ACK;
//	return dnet_send_reply(st, cmd, response.data(), response.size(), 0, context);
//}
//
//static int dnet_cmd_cache_io_remove_new(struct cache_manager *cache,
//                                        struct dnet_cmd *cmd,
//                                        void *data,
//                                        struct dnet_cmd_stats *cmd_stats,
//                                        dnet_access_context *context) {
//	using namespace ell;
//
//	auto request = [&data, &cmd] () {
//		dnet_remove_request request;
//		deserialize(data_pointer::from_raw(data, cmd->size), request);
//		return request;
//	} ();
//
//	if (request.ioflags & DNET_IO_FLAGS_NOCACHE) {
//		return -ENOTSUP;
//	}
//
//	const int err = cache->remove(cmd, request, context);
//	if (!err) {
//		cmd_stats->handled_in_cache = 1;
//	}
//	return err;
//}

int dnet_cmd_cache_io(struct dnet_backend *backend,
                      struct dnet_net_state *st,
                      struct common_request *common_req,
                      struct dnet_cmd_stats *cmd_stats,
                      struct dnet_access_context *context) {
	auto cmd = &common_req->cmd;
	if (cmd->flags & DNET_FLAGS_NOCACHE)
		return -ENOTSUP;

	auto cache = backend->cache();

	FORMATTED(HANDY_TIMER_SCOPE, ("cache.%s", dnet_cmd_string(cmd->cmd)));

	try {
		switch (cmd->cmd) {
		case DNET_CMD_WRITE_NEW:
			return dnet_cmd_cache_io_write_new(cache, st,
				                           static_cast<ell::dnet_write_request*>(common_req),
			                                   cmd_stats, context);
		case DNET_CMD_READ_NEW:
			return dnet_cmd_cache_io_read_new(cache, st,
				                          static_cast<ell::dnet_read_request*>(common_req),
			                                  cmd_stats, context);
// TODO(sabramkin): repair
//
//		case DNET_CMD_LOOKUP_NEW:
//			return dnet_cmd_cache_io_lookup_new(cache, st, cmd, cmd_stats, context);
//		case DNET_CMD_DEL_NEW:
//			return dnet_cmd_cache_io_remove_new(cache, cmd, data, cmd_stats, context);
		default:
			return -ENOTSUP;
		}
	} catch (const std::exception &e) {
		DNET_LOG_ERROR(st->n, "{}: {} cache operation failed: {}", dnet_dump_id(&cmd->id),
		               dnet_cmd_string(cmd->cmd), e.what());
		return -ENOENT;
	}
}
