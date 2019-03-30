#pragma once

#ifdef __cplusplus
extern "C" {
#endif

struct dnet_cmd;
struct dnet_io_req;

struct dnet_cmd *n2_io_req_get_cmd(struct dnet_io_req *r);
struct dnet_cmd *n2_io_req_get_request_cmd_inplace(struct dnet_io_req *r);

#ifdef __cplusplus
}
#endif
