#pragma once

#include "iserialized.hpp"
#include "iserializer.hpp"
#include "msgpack_conv.hpp"


namespace ioremap { namespace elliptics {

struct data_in_file

class serialized_raw : public iserialized {
public:
	serialized_raw(data_pointer &&, data_in_file);

	virtual void send() override;
	virtual std::unique_ptr<common_request> deserialize() override;

private:
	// Assumed, that sequence is: (raw_data, in_file_data)
	data_pointer raw_data_;
	data_in_file in_file_data_;
};

class serializer_raw : public iserializer {
public:
	virtual std::unique_ptr<iserialized> serialize(dnet_read_request &&) override;
	virtual std::unique_ptr<iserialized> serialize(dnet_read_response &&) override;
	virtual std::unique_ptr<iserialized> serialize(dnet_write_request &&) override;
	virtual std::unique_ptr<iserialized> serialize(dnet_lookup_response &&) override;
};

//

serialized_raw::serialized_raw(data_pointer && data_pointer_, data_in_file data_in_file_)
	: raw_data_(std::move(data_pointer_))
	, in_file_data_(data_in_file_)
{}

void serialized_raw::send() {
	// TODO
}

//

void put_dnet_cmd(dnet_cmd &dest, const dnet_cmd_native &src) {
	dest.id = src.id;
	dest.status = src.status;
	dest.cmd = src.cmd;
	dest.backend_id = src.backend_id;
	dest.trace_id = src.trace_id;
	dest.flags = src.flags;
	dest.trans = src.trans;
	dest.size = src.size;
}

//

std::unique_ptr<common_request> serialized_raw::deserialize() {
	// TODO: switch (cmd) ..
	return nullptr;
}

std::unique_ptr<iserialized> serializer_raw::serialize(dnet_read_request &&) {
	// TODO
	return nullptr;
}

std::unique_ptr<iserialized> serializer_raw::serialize(dnet_read_response &&resp) {
	auto header = serialize(resp);
	auto mem_block = data_pointer::allocate(sizeof(dnet_cmd) + header.size() + json.size());

	put_dnet_cmd(static_cast<dnet_cmd>(mem_block.data()), resp.cmd);
	memcpy(mem_block.skip(sizeof(dnet_cmd)).data(), header.data(), header.size());

	if (!resp.json.empty()) {
		memcpy(mem_block.skip(sizeof(dnet_cmd) + header.size()).data(), json.data(), json.size());
	}

	return serialized_raw(mem_block, resp.data);
}

std::unique_ptr<iserialized> serializer_raw::serialize(dnet_write_request &&) {
	// TODO
	return nullptr;
}

std::unique_ptr<iserialized> serializer_raw::serialize(dnet_lookup_response &&) {
	// TODO
	return nullptr;
}

}} // namespace ioremap::elliptics
