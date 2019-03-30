#pragma once

#include <unordered_map>
#include <unordered_set>

#include "library/n2_protocol.hpp"
#include "translate_request.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

class old_protocol : public protocol_interface {
public:
	old_protocol();

	// Server side
	void subscribe_request(int cmd, on_request_t on_request) override;

	// Client side
	void send_request(dnet_net_state *st,
	                  std::unique_ptr<n2_request> request,
	                  n2_repliers repliers) override;

	// Back door (receive data from net, called from C)
	int recv_message(dnet_cmd *cmd, dnet_net_state *st);

private:
	bool is_supported_message(dnet_cmd *cmd, dnet_net_state *st);
	int continue_read_message_after_cmd(dnet_cmd *cmd,
	                                    dnet_net_state *st,
	                                    data_pointer &message_buffer);

	std::unordered_map<int/*cmd*/, on_request_t> subscribe_table_;
	lookup_request_translator lookup_request_translator_;

	std::unordered_map<uint64_t/*trans_id*/, n2_repliers> repliers_table_;
};

}}} // namespace ioremap::elliptics::n2

extern "C" {

struct n2_old_protocol_io {
	n2_old_protocol_io();

	ioremap::elliptics::n2::old_protocol protocol;
};

} // extern "C"
