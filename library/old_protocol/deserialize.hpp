#pragma once

#include <memory>

#include "library/n2_protocol.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

std::unique_ptr<lookup_request> deserialize_lookup_request(const dnet_cmd &cmd);
std::unique_ptr<lookup_response> deserialize_lookup_response(const dnet_cmd &cmd, data_pointer &&message_buffer);

}}} // namespace ioremap::elliptics::n2
