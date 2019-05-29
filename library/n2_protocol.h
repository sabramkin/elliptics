#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// Helpers for integration with C

struct dnet_cmd;
struct dnet_io_req;
struct n2_request_info;
struct n2_response_info;

struct dnet_cmd *n2_io_req_get_cmd(struct dnet_io_req *r);
int n2_io_req_set_request_backend_id(struct dnet_io_req *r, int backend_id);
int n2_io_req_call_response_holder(struct dnet_io_req *r);

struct dnet_cmd *n2_request_info_get_cmd(struct n2_request_info *req_info);

void n2_request_info_free(struct n2_request_info *req_info);
void n2_response_info_free(struct n2_response_info *resp_info);

#ifdef __cplusplus
}
#endif
