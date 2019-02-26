#ifndef ELLIPTICS_PROTOCOL_H
#define ELLIPTICS_PROTOCOL_H

#ifdef __cplusplus
extern "C" {
#endif

struct dnet_cmd;
struct common_request;

struct dnet_cmd *access_cmd(struct common_request*);
void common_request_free(struct common_request *common_req);

#ifdef __cplusplus
}
#endif

#endif // ELLIPTICS_PROTOCOL_H
