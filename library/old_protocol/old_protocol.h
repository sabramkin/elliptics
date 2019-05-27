#pragma once

#ifdef __cplusplus
extern "C" {
#endif

struct dnet_cmd;
struct dnet_net_state;
struct dnet_node;

int n2_old_protocol_io_start(struct dnet_node *n);
void n2_old_protocol_io_stop(struct dnet_node *n);

int n2_old_protocol_rcvbuf_create(struct dnet_net_state *st);
void n2_old_protocol_rcvbuf_destroy(struct dnet_net_state *st);
int n2_old_protocol_try_prepare(struct dnet_net_state *st);
int n2_old_protocol_schedule_message(struct dnet_net_state *st);

#ifdef __cplusplus
}
#endif
