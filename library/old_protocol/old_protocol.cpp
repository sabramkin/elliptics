#include "old_protocol.hpp"

#include <blackhole/attribute.hpp>

#include "deserialize.hpp"
#include "library/common.hpp"
#include "library/elliptics.h"
#include "library/logger.hpp"
#include "library/tests.h"

namespace ioremap { namespace elliptics { namespace n2 {

int old_protocol::send_request(dnet_net_state *st,
                               std::unique_ptr<n2_request> request,
                               n2_repliers repliers) {
	auto &cmd = request->cmd;

	// Create transaction

	timespec wait_ts;
	auto deadline = request->deadline;
	if (dnet_time_is_empty(&deadline)) {
		wait_ts = { .tv_sec = DNET_DEFAULT_WAIT_TIMEOUT_SEC, .tv_nsec = 0 };
	} else {
		wait_ts = dnet_time_left_to_timeout(deadline);
	}

	if (!wait_ts.tv_sec && !wait_ts.tv_nsec) {
		return -ETIMEDOUT;
	}

	dnet_trans *t = dnet_trans_alloc(st->n, 0);
	if (!t) {
		return -ENOMEM;
	}

	cmd.trans = t->cmd.trans = t->trans = atomic_inc(&st->n->trans);

	t->cmd = cmd;
	t->command = cmd.cmd;
	t->st = dnet_state_get(st);
	t->wait_ts = wait_ts;
	t->repliers = new n2_repliers(std::move(repliers));

	pthread_mutex_lock(&st->trans_lock);
	int err = dnet_trans_insert_nolock(st, t);
	if (err) {
		dnet_trans_put(t);
	} else {
		dnet_trans_update_timestamp(t);
		dnet_trans_insert_timer_nolock(st, t);
	}
	pthread_mutex_unlock(&st->trans_lock);
	if (err)
		return err;

	dnet_test_settings test_settings;
	if (t->n->test_settings && !dnet_node_get_test_settings(t->n, &test_settings) &&
	    test_settings.commands_mask & (1 << t->command)) {
		return err;
	}

	// Send request

	switch (cmd.cmd) {
	case DNET_CMD_LOOKUP_NEW:
		return enqueue_net(st, serialize_lookup_request(std::move(request)));
	default:
		return -EINVAL;
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
	std::unique_ptr<dnet_trans, void (*)(dnet_trans *)>
		t(dnet_trans_search(st, cmd.trans), &dnet_trans_put);

	if (!t || !t->repliers)
		return -EINVAL;

	auto &repliers = *t->repliers;

	if (cmd.status) {
		return repliers.on_reply_error(cmd.status);

	} else {
		switch (cmd.cmd) {
		case DNET_CMD_LOOKUP_NEW:
			{
				std::unique_ptr<n2_message> msg;
				int err = deserialize_lookup_response(msg, st, cmd, std::move(body));
				if (err)
					return err;

				return repliers.on_reply(std::move(msg));
			}
		default:
			// Must never reach this code, due to is_supported_message() filter called before
			return -ENOTSUP;
		}
	}
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

bool n2_old_protocol_is_supported_message(struct dnet_net_state *st) {
	const dnet_cmd *cmd = &st->rcv_cmd;

	// Replies addressed to client are currently passed via old mechanic. This condition branch will be removed
	// after client supports new mechanic for DNET_CMD_LOOKUP_NEW command.
	if (cmd->flags & DNET_FLAGS_REPLY) {
		std::unique_ptr<dnet_trans, void (*)(dnet_trans *)>
		        t(dnet_trans_search(st, cmd->trans), &dnet_trans_put);

		if (t && !t->repliers)
			return false;
	}

	return cmd->cmd == DNET_CMD_LOOKUP_NEW;
}

int n2_old_protocol_prepare_message_buffer(struct dnet_net_state *st) {
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
