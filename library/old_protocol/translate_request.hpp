#pragma once

#include <atomic>

#include "library/n2_protocol.hpp"
#include "serialize.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

class replier_base {
public:
	replier_base(const char *handler_name, dnet_net_state *st, const dnet_cmd &cmd);
	int reply(std::unique_ptr<n2_message> msg);
	int reply_error(int errc);

protected:
	dnet_net_state *st_;

private:
	virtual void reply_impl(std::unique_ptr<n2_message> msg);
	void reply_error_impl(int errc);

	const char *handler_name_;
	std::atomic_flag reply_has_sent_;

	dnet_cmd cmd_;
};

class request_translator_base {
public:
	explicit request_translator_base(protocol_interface::on_request_t &on_request);
	virtual ~request_translator_base() = default;

protected:
	void finalize_and_do_callback(dnet_net_state *st, std::unique_ptr<n2_request_info> &&request_info);

	protocol_interface::on_request_t &on_request_;
};

// Lookup request stuff

class lookup_replier : public replier_base {
public:
	lookup_replier(dnet_net_state *st, const dnet_cmd &cmd);

private:
	void reply_impl(std::unique_ptr<n2_message> msg) override;
};

class lookup_request_translator : public request_translator_base {
public:
	explicit lookup_request_translator(protocol_interface::on_request_t &on_request);
	void translate_request(dnet_net_state *st, const dnet_cmd &cmd);
};

}}} // namespace ioremap::elliptics::n2
