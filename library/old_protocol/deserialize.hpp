#pragma once

#include <memory>

#include "library/n2_protocol.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

int deserialize_lookup_request(std::unique_ptr<n2_request> &out_deserialized, dnet_net_state *st,
                               const dnet_cmd &cmd);
int deserialize_lookup_response(std::unique_ptr<n2_message> &out_deserialized, dnet_net_state *st,
                                const dnet_cmd &cmd, data_pointer &&message_buffer);

}}} // namespace ioremap::elliptics::n2
