#include "heplers.h"

#include "common.hpp"
#include "elliptics.h"
#include "elliptics/interface.h"
#include "old_protocol/old_protocol.hpp"

extern "C" {

int n2_old_protocol_rcvbuf_create(struct dnet_net_state *st) {
	st->rcv_buffer = new n2_recv_buffer;
	return 0;
}

void n2_old_protocol_rcvbuf_destroy(struct dnet_net_state *st) {
	delete st->rcv_buffer;
	st->rcv_buffer = nullptr;
}

void n2_serialized_free(struct n2_serialized *serialized) {
	delete serialized;
}

void n2_trans_destroy_repliers(struct dnet_trans *t) {
	if (t->repliers && t->repliers->on_reply_error) {
		t->repliers->on_reply_error(-ECANCELED);
	}

	delete t->repliers;
}

} // extern "C"
