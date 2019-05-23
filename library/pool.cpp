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

int n2_io_req_set_request_backend_id(struct dnet_io_req *r, int backend_id) {
	if (r->io_req_type == DNET_IO_REQ_TYPED_REQUEST && r->request_info->request) {
		// TODO: maybe to have shared_ptr on cmd and not to have two copies?
		// Modify read-only long-lived cmd info
		r->request_info->cmd.backend_id = backend_id;
		// Modify cmd info within request
		r->request_info->request->cmd.backend_id = backend_id;
		return 0;
	} else {
		return -EINVAL;
	}
}

} // extern "C"
