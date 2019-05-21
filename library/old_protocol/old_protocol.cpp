#include "old_protocol.hpp"

#include <blackhole/attribute.hpp>

#include "deserialize.hpp"
#include "library/common.hpp"
#include "library/elliptics.h"
#include "library/logger.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

old_protocol::old_protocol()
: lookup_request_translator_(subscribe_table_[DNET_CMD_LOOKUP_NEW])
{}

void old_protocol::subscribe_request(int cmd, on_request_t on_request) {
	subscribe_table_.at(cmd) = std::move(on_request);
}

void old_protocol::send_request(dnet_net_state *st,
                                std::unique_ptr<n2_request> request,
                                n2_repliers repliers) {
	auto &cmd = request->cmd;
	repliers_table_.emplace(uint64_t(cmd.trans), std::move(repliers));

	switch (cmd.cmd) {
	case DNET_CMD_LOOKUP_NEW:
		enqueue_net(st, serialize_lookup_request(std::move(request)));
		break;
	default:
		throw std::domain_error("old_protocol::send_request is not supported for "
		                        "cmd = " + std::to_string(cmd.cmd));
	}
}

int old_protocol::recv_message(dnet_cmd *cmd, dnet_net_state *st) {
	if (!is_supported_message(cmd, st))
		return -ENOTSUP;

	dnet_logger_unset_trace_id();
	dnet_logger_set_trace_id(cmd->trace_id, cmd->flags & DNET_FLAGS_TRACE_BIT);

	data_pointer message_buffer;
	int err = continue_read_message_after_cmd(cmd, st, message_buffer);
	if (err)
		return err;

	// This is analogue of ending-part of pool.c/dnet_process_recv_single
	clock_gettime(CLOCK_MONOTONIC_RAW, &st->rcv_finish_ts);

	if (cmd->flags & DNET_FLAGS_REPLY) {
		auto it = repliers_table_.find(cmd->trans);
		auto &repliers = it->second;

		if (cmd->status) {
			err = repliers.on_reply_error(cmd->status);

		} else {
			switch (cmd->cmd) {
			case DNET_CMD_LOOKUP_NEW:
				err = repliers.on_reply(deserialize_lookup_response(*cmd, std::move(message_buffer)));
				break;
			default:
				// Must never reach this code, due to is_supported_message() filter called before
				err = -ENOTSUP;
			}
		}

		repliers_table_.erase(it);

	} else {
		switch (cmd->cmd) {
		case DNET_CMD_LOOKUP_NEW:
			lookup_request_translator_.translate_request(st, *cmd);
			break;
		default:
			// Must never reach this code, due to is_supported_message() filter called before
			err = -ENOTSUP;
		}
	}

	return err;

	// Some additional actions are done in caller: at pool.c/dnet_process_recv_single/label 'out'.
}

bool old_protocol::is_supported_message(dnet_cmd *cmd, dnet_net_state *st) {
	if (cmd->flags & DNET_FLAGS_REPLY) {
		if (st->n2_tmp_forwarding_in_progress) {
			if (!(cmd->flags & DNET_FLAGS_MORE))
				st->n2_tmp_forwarding_in_progress = 0;

			return cmd->cmd == DNET_CMD_LOOKUP_NEW;
		} else {
			return false;
		}
	} else {
		return cmd->cmd == DNET_CMD_LOOKUP_NEW;
	}
}

int old_protocol::continue_read_message_after_cmd(dnet_cmd *cmd,
                                                  dnet_net_state *st,
                                                  data_pointer &message_buffer) {
	if (cmd->size == 0)
		return 0;

	message_buffer = data_pointer::allocate(cmd->size);

	char *read_ptr = message_buffer.data<char>();
	size_t read_size = cmd->size;

	while (read_size) {
		int recv_result = recv(st->read_s, read_ptr, read_size, 0);

		if (recv_result < 0) {
			if (errno != EAGAIN && errno != EINTR) {
				DNET_LOG_ERROR(st->n, "%s: failed to receive data, socket: %d/%d", dnet_state_dump_addr(st),
					       st->read_s, st->write_s);
				return -errno;
			}

			return -EAGAIN;
		}

		if (recv_result == 0) {
			DNET_LOG_ERROR(st->n, "%s: peer has disconnected, socket: %d/%d",
				       dnet_state_dump_addr(st), st->read_s, st->write_s);
			return -ECONNRESET;
		}

		read_ptr += recv_result;
		read_size -= recv_result;
	}

	return 0;
}

}}} // namespace ioremap::elliptics::n2

extern "C" {

n2_old_protocol_io::n2_old_protocol_io() {
	// The outer part from protocol: protocol gave us request, and we must put it to request_queue
	auto on_request = [](dnet_net_state *st, std::unique_ptr<n2_request_info> request_info) {
		auto r = static_cast<dnet_io_req *>(calloc(1, sizeof(dnet_io_req)));
		if (!r)
			throw std::bad_alloc();

		r->io_req_type = DNET_IO_REQ_TYPED_REQUEST;
		r->request_info = request_info.release();

		r->st = dnet_state_get(st);
		dnet_schedule_io(st->n, r);
	};

	for (int cmd : { DNET_CMD_LOOKUP_NEW })
		protocol.subscribe_request(cmd, on_request);
}

int n2_old_protocol_io_start(struct dnet_node *n) {
	auto impl = [io = n->io] {
		io->old_protocol = new n2_old_protocol_io;
		return 0;
	};
	return c_exception_guard(impl, n, __FUNCTION__);
}

int n2_old_protocol_io_stop(struct dnet_node *n) {
	auto impl = [io = n->io] {
		delete io->old_protocol;
		io->old_protocol = nullptr;
		return 0;
	};
	return c_exception_guard(impl, n, __FUNCTION__);
}

int n2_old_protocol_recv_body(struct dnet_cmd *cmd, struct dnet_net_state *st) {
	auto impl = [&] {
		return st->n->io->old_protocol->protocol.recv_message(cmd, st);
	};
	return c_exception_guard(impl, st->n, __FUNCTION__);
}

} // extern "С"
