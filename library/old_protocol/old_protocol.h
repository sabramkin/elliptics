#pragma once

#ifdef __cplusplus
extern "C" {
#endif

struct dnet_cmd;
struct dnet_net_state;
struct dnet_node;

int n2_old_protocol_io_start(struct dnet_node *n);
int n2_old_protocol_io_stop(struct dnet_node *n);

int n2_old_protocol_recv_body(struct dnet_cmd *cmd, struct dnet_net_state *st);

#ifdef __cplusplus
}
#endif
