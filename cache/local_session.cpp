/*
 * Copyright 2013+ Ruslan Nigmatullin <euroelessar@yandex.ru>
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

#include "local_session.h"

#include <blackhole/attribute.hpp>

#include "library/backend.h"
#include "library/logger.hpp"
#include "library/msgpack_conv.hpp"
#include "library/protocol.hpp"

using namespace ioremap::elliptics;

#undef list_entry
#define list_entry(ptr, type, member) ({			\
	const list_head *__mptr = (ptr);	\
	(dnet_io_req *)( (char *)__mptr - dnet_offsetof(dnet_io_req, member) );})

#undef list_for_each_entry_safe
#define list_for_each_entry_safe(pos, n, head, member)			\
	for (pos = list_entry((head)->next, decltype(*pos), member),	\
		n = list_entry(pos->member.next, decltype(*pos), member);	\
	     &pos->member != (head); 					\
	     pos = n, n = list_entry(n->member.next, decltype(*n), member))

local_session::local_session(dnet_backend &backend, dnet_node *node)
: m_backend(backend)
, m_ioflags(DNET_IO_FLAGS_CACHE)
, m_cflags(DNET_FLAGS_NOLOCK) {
	m_state = reinterpret_cast<dnet_net_state *>(malloc(sizeof(dnet_net_state)));
	if (!m_state)
		throw std::bad_alloc();

	memset(m_state, 0, sizeof(dnet_net_state));

	m_state->__need_exit = -1;
	m_state->write_s = -1;
	m_state->read_s = -1;
	m_state->accept_s = -1;

	dnet_state_micro_init(m_state, node, node->addrs, 0);
	dnet_state_get(m_state);
}

local_session::~local_session()
{
	dnet_state_put(m_state);
	dnet_state_put(m_state);
}

void local_session::set_ioflags(uint32_t flags)
{
	m_ioflags = flags;
}

void local_session::set_cflags(uint64_t flags)
{
	m_cflags = flags;
}

int local_session::read(const dnet_id &id,
                        uint64_t *user_flags,
                        data_pointer *json,
                        dnet_time *json_ts,
                        data_pointer *data,
                        dnet_time *data_ts) {
	const uint64_t read_flags = (json ? DNET_READ_FLAGS_JSON : 0) | (data ? DNET_READ_FLAGS_DATA : 0);

	dnet_read_request request;

	dnet_cmd &cmd = request.cmd;
	memset(&cmd, 0, sizeof(cmd));
	cmd.id = id;
	cmd.cmd = DNET_CMD_READ_NEW;
	cmd.flags |= m_cflags;
	cmd.backend_id = m_backend.backend_id();

	request.ioflags = m_ioflags;
	request.read_flags = read_flags;
	request.data_offset = 0;
	request.data_size = 0;
	request.deadline = dnet_time{0, 0};

	const int err = dnet_process_cmd_raw(m_state, &request, 0, 0, /*context*/ nullptr);
	if (err) {
		clear_queue();
		return err;
	}

	struct dnet_io_req *r, *tmp;

	list_for_each_entry_safe(r, tmp, &m_state->send_list, req_entry) {
		dnet_read_response *response = static_cast<dnet_read_response *>(r->common_req);
		dnet_cmd *req_cmd = &response->cmd;

		DNET_LOG_DEBUG(m_state->n, "entry in list, status: {}", req_cmd->status);

		if (req_cmd->status) {
			clear_queue();
			return req_cmd->status;
		} else {
			if (user_flags)
				*user_flags = response->user_flags;
			if (json_ts)
				*json_ts = response->json_timestamp;
			if (data_ts)
				*data_ts = response->data_timestamp;

			// TODO(sabramkin): log data size
			DNET_LOG_DEBUG(m_state->n, "entry in list, json_size: {}, data_size: ?", response->json.size());

			if (json) {
				*json = data_pointer::copy(response->json);
			}

			if (data) {
				data_pointer result;

				if (response->data.where() == data_place::IN_FILE) {
					result = data_pointer::allocate(response->read_data_size);

					auto &in_file = response->data.in_file;
					const ssize_t err = dnet_read_ll(in_file.fd, result.data<char>(),
					                                 result.size(), in_file.local_offset);
					if (err) {
						clear_queue();
						return err;
					}

				} else if (response->data.where() == data_place::IN_MEMORY) {
					result = data_pointer::copy(response->data.in_memory);
				} else {
					result = data_pointer();
				}

				clear_queue();
				*data = std::move(result);
			}

			return 0;
		}
	}

	clear_queue();
	return -ENOENT;
}

int local_session::write(const dnet_id &id,
                         uint64_t user_flags,
                         const std::string &json,
                         const dnet_time &json_ts,
                         const std::string &data,
                         const dnet_time &data_ts) {
	dnet_write_request request;

	dnet_cmd &cmd = request.cmd;
	memset(&cmd, 0, sizeof(cmd));
	cmd.id = id;
	cmd.cmd = DNET_CMD_WRITE_NEW;
	cmd.flags = m_cflags;
	cmd.backend_id = m_backend.backend_id();

	request.ioflags = m_ioflags | DNET_IO_FLAGS_PREPARE | DNET_IO_FLAGS_COMMIT | DNET_IO_FLAGS_PLAIN_WRITE;
	request.user_flags = user_flags;
	request.timestamp = data_ts;
	request.json_size = json.size();
	request.json_capacity = json.capacity();
	request.json_timestamp = json_ts;
	request.data_offset = 0;
	request.data_size = data.size();
	request.data_capacity = data.size();
	request.data_commit_size = data.size();
	request.cache_lifetime = 0;
	request.deadline = {0, 0};

	request.json = data_pointer::allocate(json.size());
	memcpy(request.json.data(), json.data(), json.size());

	request.data = data_pointer::allocate(data.size());
	memcpy(request.data.data(), data.data(), data.size());

	DNET_LOG_DEBUG(m_state->n, "going to write: json_size: {}, data_size: {}",
	               request.json.size(), request.data.size());

	int err = dnet_process_cmd_raw(m_state, &request, 0, 0, /*context*/ nullptr);
	clear_queue(&err);
	return err;
}

// TODO(sabramkin): uncomment and repair
//data_pointer local_session::lookup(const dnet_cmd &tmp_cmd, int *errp)
//{
//	dnet_cmd cmd = tmp_cmd;
//	cmd.flags |= m_cflags;
//	cmd.size = 0;
//	cmd.backend_id = m_backend.backend_id();
//
//	*errp = dnet_process_cmd_raw(m_state, &cmd, nullptr, 0, 0, /*context*/ nullptr);
//
//	if (*errp)
//		return data_pointer();
//
//	struct dnet_io_req *r, *tmp;
//
//	list_for_each_entry_safe(r, tmp, &m_state->send_list, req_entry) {
//		dnet_cmd *req_cmd = reinterpret_cast<dnet_cmd *>(r->header ? r->header : r->data);
//
//		if (req_cmd->status) {
//			*errp = req_cmd->status;
//			clear_queue();
//			return data_pointer();
//		} else if (req_cmd->size) {
//			data_pointer result = data_pointer::copy(req_cmd + 1, req_cmd->size);
//			clear_queue();
//			return result;
//		}
//	}
//
//	*errp = -ENOENT;
//	clear_queue();
//	return data_pointer();
//}

int local_session::remove_new(const struct dnet_id &id,
                              const dnet_remove_request *request,
                              dnet_access_context *context) {
	dnet_remove_request forward_request(*request);
	struct dnet_cmd &cmd = forward_request.cmd;
	memset(&cmd, 0, sizeof(struct dnet_cmd));
	cmd.id = id;
	cmd.flags = DNET_FLAGS_NOLOCK;
	cmd.cmd = DNET_CMD_DEL_NEW;
	cmd.backend_id = m_backend.backend_id();

	struct dnet_cmd_stats cmd_stats;
	memset(&cmd_stats, 0, sizeof(struct dnet_cmd_stats));

	const auto &callbacks = m_backend.callbacks();
	int err = callbacks.command_handler(m_state,
	                                    callbacks.command_private,
	                                    &forward_request,
	                                    &cmd_stats,
	                                    context);
	DNET_LOG_NOTICE(m_state->n, "{}: local remove_new: err: {}", dnet_dump_id(&cmd.id), err);

	return err;
}

void local_session::clear_queue(int *errp)
{
	struct dnet_io_req *r, *tmp;

	list_for_each_entry_safe(r, tmp, &m_state->send_list, req_entry) {
		dnet_cmd *cmd = reinterpret_cast<dnet_cmd *>(r->header ? r->header : r->data);

		if (errp && cmd->status)
			*errp = cmd->status;

		list_del(&r->req_entry);
		dnet_io_req_free(r);
	}
}
