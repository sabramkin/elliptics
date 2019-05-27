#include "old_protocol.hpp"

#include <blackhole/attribute.hpp>

#include "deserialize.hpp"
#include "library/common.hpp"
#include "library/elliptics.h"
#include "library/logger.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

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

int old_protocol::recv_message(dnet_net_state *st, const dnet_cmd &cmd, data_pointer &&body) {
	if (cmd.flags & DNET_FLAGS_REPLY) {
		return recv_response(st, cmd, std::move(body));
	} else {
		return recv_request(st, cmd, std::move(body));
	}
}

int old_protocol::recv_request(dnet_net_state *st, const dnet_cmd &cmd, data_pointer &&body) {
	switch (cmd.cmd) {
	case DNET_CMD_LOOKUP_NEW:
		return translate_lookup_request(st, cmd);
	default:
		// Must never reach this code, due to is_supported_message() filter called before
		return -ENOTSUP;
	}
}

int old_protocol::recv_response(dnet_net_state *st, const dnet_cmd &cmd, data_pointer &&body) {
	int err = 0;
	auto it = repliers_table_.find(cmd.trans);
	auto &repliers = it->second;

	if (cmd.status) {
		err = repliers.on_reply_error(cmd.status);

	} else {
		switch (cmd.cmd) {
		case DNET_CMD_LOOKUP_NEW:
			{
				std::unique_ptr<n2_message> msg;
				err = deserialize_lookup_response(msg, st, cmd, std::move(body));
				if (err)
					break;

				err = repliers.on_reply(std::move(msg));
			}
			break;
		default:
			// Must never reach this code, due to is_supported_message() filter called before
			err = -ENOTSUP;
		}
	}

	repliers_table_.erase(it);
	return err;
}

int old_protocol::schedule_request_info(dnet_net_state *st,
                                        std::unique_ptr<n2_request_info> &&request_info) {
	request_info->cmd = request_info->request->cmd;
	return on_request(st, std::move(request_info));
}

int old_protocol::translate_lookup_request(dnet_net_state *st, const dnet_cmd &cmd) {
	std::unique_ptr<n2_request_info> request_info(new n2_request_info);

	int err = deserialize_lookup_request(request_info->request, st, cmd);
	if (err)
		return err;

	auto replier = std::make_shared<lookup_replier>(st, request_info->request->cmd);
	request_info->repliers.on_reply = std::bind(&lookup_replier::reply, replier, std::placeholders::_1);
	request_info->repliers.on_reply_error = std::bind(&lookup_replier::reply_error, replier, std::placeholders::_1);

	return schedule_request_info(st, std::move(request_info));
}

}}} // namespace ioremap::elliptics::n2

extern "C" {

int n2_old_protocol_io_start(struct dnet_node *n) {
	auto impl = [io = n->io] {
		io->old_protocol = new n2_old_protocol_io;
		return 0;
	};
	return c_exception_guard(impl, n, __FUNCTION__);
}

void n2_old_protocol_io_stop(struct dnet_node *n) {
	auto impl = [io = n->io] {
		delete io->old_protocol;
		io->old_protocol = nullptr;
		return 0;
	};
	c_exception_guard(impl, n, __FUNCTION__);
}

int n2_old_protocol_rcvbuf_create(struct dnet_net_state *st) {
	st->rcv_buffer = new n2_recv_buffer;
	return 0;
}

void n2_old_protocol_rcvbuf_destroy(struct dnet_net_state *st) {
	delete st->rcv_buffer;
	st->rcv_buffer = nullptr;
}

bool n2_old_protocol_is_supported_message(struct dnet_net_state *st) {
	const dnet_cmd *cmd = &st->rcv_cmd;

	if (cmd->flags & DNET_FLAGS_REPLY) {
		// TODO(sabramkin): move n2_tmp_forwarding_in_progress flag to transaction descriptor
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

int n2_old_protocol_try_prepare(struct dnet_net_state *st) {
	if (!n2_old_protocol_is_supported_message(st)) {
		st->rcv_buffer_used = 0;
		return -ENOTSUP;
	} else {
		st->rcv_buffer_used = 1;
	}

	const dnet_cmd *cmd = &st->rcv_cmd;

	st->rcv_buffer->buf = ioremap::elliptics::data_pointer::allocate(cmd->size);
	st->rcv_data = st->rcv_buffer->buf.data();
	st->rcv_offset = 0;
	st->rcv_end = cmd->size;
	return 0;
}

int n2_old_protocol_schedule_message(struct dnet_net_state *st) {
	if (!st->rcv_buffer_used)
		return -ENOTSUP;

	auto impl = [&] {
		return st->n->io->old_protocol->protocol.recv_message(st, st->rcv_cmd, std::move(st->rcv_buffer->buf));
	};
	return c_exception_guard(impl, st->n, __FUNCTION__);
}

} // extern "С"
