#include "translate_request.hpp"

#include <blackhole/attribute.hpp>
#include <msgpack.hpp>

#include "deserialize.hpp"
#include "library/common.hpp"
#include "library/elliptics.h"

namespace ioremap { namespace elliptics { namespace n2 {

replier_base::replier_base(const char *handler_name, dnet_net_state *st, const dnet_cmd &cmd)
: st_(st)
, handler_name_(handler_name)
, reply_has_sent_(ATOMIC_FLAG_INIT)
, cmd_(cmd)
{}

int replier_base::reply(std::unique_ptr<n2_message> msg) {
	auto impl = [&] {
		msg->cmd.trans = cmd_.trans;
		reply_impl(std::move(msg));
		return 0;
	};

	if (!reply_has_sent_.test_and_set()) {
		return c_exception_guard(impl, st_->n, __FUNCTION__);
	} else {
		return -EALREADY;
	}
}

int replier_base::reply_error(int errc) {
	auto impl = [&] {
		reply_error_impl(errc);
		return 0;
	};

	if (!reply_has_sent_.test_and_set()) {
		return c_exception_guard(impl, st_->n, __FUNCTION__);
	} else {
		return -EALREADY;
	}
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

lookup_replier::lookup_replier(dnet_net_state *st, const dnet_cmd &cmd)
: replier_base("LOOKUP_NEW", st, cmd)
{}

void lookup_replier::reply_impl(std::unique_ptr<n2_message> msg) {
	enqueue_net(st_, serialize_lookup_response(std::move(msg)));
}

}}} // namespace ioremap::elliptics::n2

void n2_serialized_free(struct n2_serialized *serialized) {
	delete serialized;
}
