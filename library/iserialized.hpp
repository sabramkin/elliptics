#pragma once

namespace ioremap { namespace elliptics {

class iserialized {
public:
	virtual ~iserialized() = default;

	virtual void send() = 0; // request/response send via net
	virtual std::unique_ptr<common_request> deserialize() = 0;
};

}} // namespace ioremap::elliptics
