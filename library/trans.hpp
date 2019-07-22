#pragma once

#include "n2_protocol.hpp"

namespace ioremap { namespace elliptics {

int n2_trans_alloc_send(dnet_session *s, n2_request_info &&request_info, dnet_addr &addr_out);

}} // ioremap::elliptics
