#pragma once

#include <atomic>

#include "library/n2_protocol.hpp"

struct n2_serialized {
	dnet_cmd cmd;

	// Typed message usually represented as some struct that references to allocated memory blocks. When we serialize,
	// we don't want to concat large blocks of data to one continuous memory block, since we don't want to copy memory.
	// Instead, we get serialized message as multi-chunk vector, each chunk ot that is a view of particular message part.
	// TODO: Assumed that sendfile'll be supported later, and n2_serialized became vector<some_variant>
	std::vector<ioremap::elliptics::data_pointer> chunks;
};

namespace ioremap { namespace elliptics { namespace n2 {

void enqueue_net(dnet_net_state *st, std::unique_ptr<n2_serialized> serialized);

// Serializators for requests and responses

std::unique_ptr<n2_serialized> serialize_error_response(const dnet_cmd &cmd);

std::unique_ptr<n2_serialized> serialize_lookup_request(std::unique_ptr<n2_request> msg);
std::unique_ptr<n2_serialized> serialize_lookup_response(std::unique_ptr<n2_message> msg);

}}} // namespace ioremap::elliptics::n2
