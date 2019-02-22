#include "write_request.hpp"


namespace grpc_dnet {

write_request_manager::write_request_manager(service_provider_info *service_provider_info_in, TRequestProcessor request_processor_in)
	: request_manager_base(service_provider_info_in)
	, request_(std::make_shared<TRequest>())
	, json_offset_(0)
	, data_offset_(0)
{
	service_->RequestWrite(ctx_.get(), &rpc_responder_, cq_, cq_, this);
}


void write_request_manager::send_response(const TResponse &response) {
	responder_.Finish(serialize(response), Status::OK, this);
}


void write_request_manager::process_completion(bool more) override {
	if (!utilization_started_) {
		utilization_started_ = true;

		// Attach next listener to completion queue
		new write_request_processor(service_provider_info_, request_processor_in);
	}

	if (!more) {
		// This case may occur only in reading state
		on_request_final();
		in_reading_state_ = false;
	}

	if (in_reading_state_) {
		on_request_available_next();
	} else {
		on_response_sent();
	}
}


void write_request_manager::on_request_available_next() { // TODO: maybe do it async
	TRpcRequest rpc_request;
	responder_.Read(&rpc_request, this);
	deserialize_next(rpc_request, *request_);
}


void write_request_manager::on_request_final() { // TODO: maybe do it async
	if (json_offset_ != request.json.size() || data_offset_ != request.data.size()) {
		throw std::invalid_argument("Incomplete buffer");
	}

	time_point_to_dnet_time(ctx_.deadline(), request_->deadline);

	request_processor_in(request_);
}


void write_request_manager::on_response_sent() {
	delete this;
}


void write_request_manager::append_data(data_pointer &total_data, size_t &current_part_offset, fb::Vector<uint8_t> *current_part) {
	if (!current_part) {
		return;
	}

	auto current_part_size = current_part->size();
	size_t next_part_offset = current_part_offset + current_part_size;
	if (next_part_offset > total_data.size()) {
		throw std::invalid_argument("Buffer overflow");
	}

	memcpy(total_data.skip(current_part_offset).data(), current_part->data(), current_part_size);
	current_part_offset = next_part_offset;
}


void write_request_manager::deserialize_next(const TRpcRequest &rpc_request, TRequest &request) {
	auto fb_request = rpc_request.GetRoot();
	auto fb_request_header = fb_request->header();
	if (fb_request_header) {
		deserialize_cmd(fb_request_header->cmd(), request.cmd);
		request.ioflags = fb_request_header->ioflags();
		request.user_flags = fb_request_header->user_flags();
		nanosec_to_dnet_time(fb_request_header->timestamp(), request.timestamp);
		request.json_size = fb_request_header->json_size();
		request.json_capacity = fb_request_header->json_capacity();
		nanosec_to_dnet_time(fb_request_header->json_timestamp(), request.json_timestamp);
		request.data_offset = fb_request_header->data_offset();
		request.data_size = fb_request_header->data_size();
		request.data_capacity = fb_request_header->data_capacity();
		request.data_commit_size = fb_request_header->data_commit_size();
		nanosec_to_dnet_time(fb_request_header->cache_lifetime(), request.cache_lifetime);

		request.json = data_pointer::allocate(request.json_size);
		request.data = data_pointer::allocate(request.data_size);
	}

	append_data(request.json, json_offset_, fb_request->json());
	append_data(request.data, data_offset_, fb_request->data());
}


static write_request_manager::TRpcResponse write_request_manager::serialize(const TResponse &response) {
	flatbuffers::grpc::MessageBuilder builder;

	auto fb_response = fb_grpc_dnet::CreateLookupResponse(
		builder,
		response.record_flags,
		response.user_flags,
		builder.CreateString(path),
		dnet_time_to_ns(response.json_timestamp),
		response.json_offset,
		response.json_size,
		response.json_capacity,
		builder.CreateVector(response.json_checksum),
		dnet_time_to_ns(response.data_timestamp),
		response.data_offset,
		response.data_size,
		builder.CreateVector(response.data_checksum)
	);

	builder.Finish(fb_response);

	return builder.ReleaseMessage<fb_grpc_dnet::WriteResponse>();
}

} // namespace grpc_dnet
