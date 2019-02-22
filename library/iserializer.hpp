#pragma once

#include "protocol.hpp"

namespace ioremap { namespace elliptics {

class iserializer {
public:
	virtual ~iserializer() = default;

	virtual std::unique_ptr<iserialized> serialize(dnet_read_request &&) = 0;
	virtual std::unique_ptr<iserialized> serialize(dnet_read_response &&) = 0;
	virtual std::unique_ptr<iserialized> serialize(dnet_write_request &&) = 0;
	virtual std::unique_ptr<iserialized> serialize(dnet_lookup_response &&) = 0;

	// TODO: uncomment pure virtual
	virtual std::unique_ptr<iserialized> serialize(dnet_remove_request &&) { return nullptr; } /*= 0;*/
	virtual std::unique_ptr<iserialized> serialize(dnet_bulk_read_request &&) { return nullptr; } /*= 0;*/
	virtual std::unique_ptr<iserialized> serialize(dnet_bulk_remove_request &&) { return nullptr; } /*= 0;*/
	virtual std::unique_ptr<iserialized> serialize(dnet_iterator_request &&) { return nullptr; } /*= 0;*/
	virtual std::unique_ptr<iserialized> serialize(dnet_iterator_response &&) { return nullptr; } /*= 0;*/
	virtual std::unique_ptr<iserialized> serialize(dnet_server_send_request &&) { return nullptr; } /*= 0;*/
};

}} // namespace ioremap::elliptics
