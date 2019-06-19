#include "repliers.hpp"

#include <blackhole/attribute.hpp>
#include <msgpack.hpp>

#include "serialize.hpp"
#include "library/common.hpp"
#include "library/elliptics.h"

namespace ioremap { namespace elliptics { namespace n2 {

replier_base::replier_base(const char *handler_name, dnet_net_state *st, const dnet_cmd &cmd)
: st_(st)
, cmd_(cmd)
, handler_name_(handler_name)
, need_ack_(!!(cmd_.flags & DNET_FLAGS_NEED_ACK))
, reply_has_sent_(ATOMIC_FLAG_INIT)
{
	cmd_.flags = (cmd_.flags & ~(DNET_FLAGS_NEED_ACK)) | DNET_FLAGS_REPLY;
}

int replier_base::reply(const std::shared_ptr<void> &msg) {
	if (!reply_has_sent_.test_and_set()) {
		return c_exception_guard(std::bind(&replier_base::reply_impl, this, std::cref(msg)),
		                         st_->n, __FUNCTION__);
	} else {
		return -EALREADY;
	}
}

int replier_base::reply_error(int errc) {
	if (!reply_has_sent_.test_and_set()) {
		return c_exception_guard(std::bind(&replier_base::reply_error_impl, this, errc),
		                         st_->n, __FUNCTION__);
	} else {
		return -EALREADY;
	}
}

int replier_base::reply_impl(const std::shared_ptr<void> &) {
	return -EINVAL;
}

int replier_base::reply_error_impl(int errc) {
	if (!need_ack_)
		return 0;

	cmd_.size = 0;
	cmd_.status = errc;

	std::unique_ptr<n2_serialized> serialized;
	int err = serialize(st_->n, cmd_, serialized);
	if (err)
		return err;

	return enqueue_net(st_, std::move(serialized));
}

lookup_replier::lookup_replier(dnet_net_state *st, const dnet_cmd &cmd)
: replier_base("LOOKUP_NEW", st, cmd)
{}

int lookup_replier::reply_impl(const std::shared_ptr<void> &msg) {
	std::unique_ptr<n2_serialized> serialized;
	int err = serialize(st_->n, cmd_, *static_cast<lookup_response *>(msg.get()), serialized);
	if (err)
		return err;

	return enqueue_net(st_, std::move(serialized));
}

}}} // namespace ioremap::elliptics::n2
