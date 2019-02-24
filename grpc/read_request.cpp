#include "write_request.hpp"


namespace grpc_dnet {

read_request_manager::read_request_manager(service_provider_info *service_provider_info_in, THandler request_processor_in)
	: request_manager_base(service_provider_info_in)
	, response_json_offset_(0)
	, response_data_offset_(0)
	, response_serialize_complete_(false)
	, response_finish_sent_(false)
	, handler_(handler_in)
{
	on_complete_op_ = on_complete_op::READ_REQUEST;
	service_->RequestRead(ctx_.get(), &rpc_request_, &rpc_responder_, cq_, cq_, this);
}


void read_request_manager::send_response(const std::shared_ptr<TResponse> &response) {
	on_complete_op_ = send_next();
}


void read_request_manager::process_completion(bool more) {
	switch (on_complete_op_) {
	case on_complete_op::HANDLE_REQUEST:
		new read_request_manager(service_provider_info_, handler_);
		handle_request();
		break;

	case on_complete_op::HANDLE_SEND_PARTIAL_COMPLETE:
		on_complete_op_ = send_next();
		break;

	case on_complete_op::HANDLE_SEND_ALL_COMPLETE:
		send_finish_marker();
		on_complete_op_ = on_complete_op::DELETE;
		break;

	case on_complete_op::DELETE:
		delete this;
		break;
	}
}


void read_request_manager::handle_request() {
	request = std::make_shared<TRequest>();
	deserialize(rpc_request_, *request);
	time_point_to_dnet_time(ctx_.deadline(), request->deadline);

	handler_(request);
}


read_request_manager::on_complete_op read_request_manager::send_next() {
	bool complete;
        responder_.Write(serialize_next(*response_, complete), this);

        if (complete) {
        	return on_complete_op::HANDLE_SEND_ALL_COMPLETE;
	} else {
        	return on_complete_op::HANDLE_SEND_PARTIAL_COMPLETE;
	}
}


void read_request_manager::send_finish_marker() {
        responder_.Finish(Status::OK, this);
}


void read_request_manager::deserialize(const TRpcRequest &rpc_request, TRequest &request) {
	auto fb_request = rpc_request.GetRoot();

	deserialize_cmd(fb_request->cmd(), request.cmd);
	request.ioflags = fb_request->ioflags();
	request.read_flags = fb_request->read_flags();
	request.data_offset = fb_request->data_offset();
	request.data_size = fb_request->data_size();
}


read_request_manager::TRpcResponse read_request_manager::serialize_next(const TResponse &response, bool &complete) {
	fb::grpc::MessageBuilder builder;

	fb::Offset<fb_grpc_dnet::ReadResponseHeader> fb_response_header = 0;
	if (!response_json_offset_ && !response_data_offset_) {
		//Only first message has header
		fb_response_header = fb_grpc_dnet::CreateReadResponseHeader(
			builder,
			serialize_cmd(builder, response.cmd),
			response.record_flags,
			response.user_flags,
			response.json_timestamp,
			response.json_size,
			response.json_capacity,
			response.read_json_size,
			response.data_timestamp,
			response.data_size,
			response.read_data_offset,
			response.read_data_size
		);
	}

	auto res = fb_grpc_dnet::CreateReadResponse(
		builder,
		fb_response_header,
		put_data_part(builder, response.json, response_json_offset_),
		put_data_part(builder, response.data, response_data_offset_)
	);

	complete = (response_json_offset_ == response.json.size() && response_data_offset_ == response.data.size());
	return res;
}


fb::Offset<fb::Vector<uint8_t>> read_request_manager::put_data_part(fb::grpc::MessageBuilder &builder,
	                                                            const data_pointer &total_data,
	                                                            size_t &current_part_offset) {
	if (current_part_offset == total_data.size()) {
		// Whole buffer already serialized
		return 0;
	}

	if (builder.GetSize() >= MAX_MESSAGE_SIZE) {
		// No place to serialize some
		return 0;
	}

	size_t max_part_size = MAX_MESSAGE_SIZE - builder.GetSize();
	size_t size_left = total_data.size() - current_part_offset;
	size_t part_size = std::min(size_left, size_left);

	auto res = builder.CreateVector(static_cast<const uint8_t *>(total_data.skip(current_part_offset)), part_size);
	current_part_offset =+ part_size;
	return res;
}

} // namespace grpc_dnet
