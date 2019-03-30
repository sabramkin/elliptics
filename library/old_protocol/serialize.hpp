#pragma once

#include <atomic>

#include "library/n2_protocol.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

// Typed message usually represented as some struct that references to allocated memory blocks. When we serialize,
// we don't want to concat large blocks of data to one continuous memory block, since we don't want to copy memory.
// Instead, we get serialized message as multi-chunk vector, each chunk ot that is a view of particular message part.
// TODO: Assumed that sendfile'll be supported later, and net_iovec became vector<some_variant>
using net_iovec = std::vector<data_pointer>;

void enqueue_net(dnet_net_state *st, net_iovec serialized);

// Serializators for requests and responses

net_iovec serialize_error_response(const dnet_cmd &cmd);

net_iovec serialize_lookup_request(std::unique_ptr<n2_request> msg);
net_iovec serialize_lookup_response(std::unique_ptr<n2_message> msg);

}}} // namespace ioremap::elliptics::n2

struct n2_net_iovec {
	ioremap::elliptics::n2::net_iovec iov;
};
