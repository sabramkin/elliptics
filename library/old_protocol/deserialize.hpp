#pragma once

#include <memory>

#include "library/n2_protocol.hpp"

namespace ioremap { namespace elliptics { namespace n2 {

dnet_cmd deserialize_error_response(data_pointer &&message_buffer);

std::unique_ptr<lookup_request> deserialize_lookup_request(data_pointer &&message_buffer);
std::unique_ptr<lookup_response> deserialize_lookup_response(data_pointer &&message_buffer);

}}} // namespace ioremap::elliptics::n2
