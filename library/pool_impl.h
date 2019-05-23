#pragma once

#ifdef __cplusplus
extern "C" {
#endif

struct dnet_cmd;
struct dnet_io_req;

struct dnet_cmd *n2_io_req_get_cmd(struct dnet_io_req *r);
int n2_io_req_set_request_backend_id(struct dnet_io_req *r, int backend_id);

#ifdef __cplusplus
}
#endif
