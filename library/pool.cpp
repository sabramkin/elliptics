#include <blackhole/attribute.hpp>

#include "common.hpp"
#include "elliptics.h"
#include "elliptics/interface.h"
#include "old_protocol/old_protocol.hpp"
#include "pool_impl.h"

extern "C" {

struct dnet_cmd *n2_io_req_get_cmd(struct dnet_io_req *r) {
	switch (r->io_req_type) {
	case DNET_IO_REQ_TYPED_REQUEST:
		return &r->request_info->cmd;
	case DNET_IO_REQ_TYPED_RESPONSE:
		return &r->response_info->cmd;
	default:
		return nullptr; // Must never reach this code
	}
}

struct dnet_cmd *n2_io_req_get_request_cmd_inplace(struct dnet_io_req *r) {
	if (r->io_req_type == DNET_IO_REQ_TYPED_REQUEST && r->request_info->request)
		return &r->request_info->request->cmd;
	else
		return nullptr; // Must never reach this code
}

} // extern "C"
