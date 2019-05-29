#pragma once

#ifdef __cplusplus
extern "C" {
#endif

struct dnet_cmd;
struct dnet_io_req;
struct dnet_trans;

int n2_old_protocol_rcvbuf_create(struct dnet_net_state *st);
void n2_old_protocol_rcvbuf_destroy(struct dnet_net_state *st);

void n2_serialized_free(struct n2_serialized *serialized);

void n2_trans_destroy_repliers(struct dnet_trans *t);

#ifdef __cplusplus
}
#endif
