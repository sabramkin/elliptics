#include "translate_request.hpp"

#include <msgpack.hpp>

#include "deserialize.hpp"
#include "library/elliptics.h"

namespace ioremap { namespace elliptics { namespace n2 {

replier_base::replier_base(const char *handler_name, dnet_net_state *st, const dnet_cmd &cmd)
: st_(st)
, handler_name_(handler_name)
, reply_has_sent_(ATOMIC_FLAG_INIT)
, cmd_(cmd)
{}

void replier_base::reply(std::unique_ptr<n2_message> msg) {
	if (!reply_has_sent_.test_and_set()) {
		msg->cmd.trans = cmd_.trans;
		reply_impl(std::move(msg));
	}
}

void replier_base::reply_error(int errc) {
	if (!reply_has_sent_.test_and_set())
		reply_error_impl(errc);
}

void replier_base::reply_impl(std::unique_ptr<n2_message> msg) {
	throw std::domain_error(std::string("reply is unimplemented for ") + handler_name_);
}

void replier_base::reply_error_impl(int errc) {
	if (!(cmd_.flags & DNET_FLAGS_NEED_ACK))
		return;

	dnet_cmd cmd = cmd_;
	cmd.size = 0;
	cmd.status = errc;

	enqueue_net(st_, serialize_error_response(cmd));
}

request_translator_base::request_translator_base(protocol_interface::on_request_t &on_request)
: on_request_(on_request)
{}

void request_translator_base::finalize_and_do_callback(dnet_net_state *st,
                                                       std::unique_ptr<n2_request_info> &&request_info) {
	request_info->cmd = request_info->request->cmd;
	on_request_(st, std::move(request_info));
}

// Lookup request stuff

lookup_replier::lookup_replier(dnet_net_state *st, const dnet_cmd &cmd)
: replier_base("LOOKUP_NEW", st, cmd)
{}

void lookup_replier::reply_impl(std::unique_ptr<n2_message> msg) {
	enqueue_net(st_, serialize_lookup_response(std::move(msg)));
}

lookup_request_translator::lookup_request_translator(protocol_interface::on_request_t &on_request)
: request_translator_base(on_request)
{}

void lookup_request_translator::translate_request(dnet_net_state *st, data_pointer message_buffer) {
	std::unique_ptr<n2_request_info> request_info(new n2_request_info);

	request_info->request = deserialize_lookup_request(std::move(message_buffer));

	auto replier = std::make_shared<lookup_replier>(st, request_info->request->cmd);
	request_info->repliers.on_reply = std::bind(&lookup_replier::reply, replier, std::placeholders::_1);
	request_info->repliers.on_reply_error = std::bind(&lookup_replier::reply_error, replier, std::placeholders::_1);

	finalize_and_do_callback(st, std::move(request_info));
}

}}} // namespace ioremap::elliptics::n2

void n2_iovec_free(struct n2_net_iovec *iov) {
	delete iov;
}
