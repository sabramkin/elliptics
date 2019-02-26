#pragma once

#include "requests_common.hpp"

namespace grpc_dnet {

class read_request_manager : public request_manager_base {
public:
	using TRequest = ell::dnet_read_request;
	using TResponse = ell::dnet_read_response;
	using THandler = std::function<void (std::unique_ptr<TRequest>)>;

public:
	read_request_manager(service_provider_info *service_provider_info_in, THandler request_processor_in);

	void send_response(std::unique_ptr<TResponse> response);

private:
	enum class on_complete_op {
		HANDLE_REQUEST,
		HANDLE_SEND_PARTIAL_COMPLETE,
		HANDLE_SEND_ALL_COMPLETE,
		DELETE,
	};

	using TRpcRequest = fb::grpc::Message<fb_grpc_dnet::ReadRequest>;
	using TRpcResponse = fb::grpc::Message<fb_grpc_dnet::ReadResponse>;

private:
	virtual void process_completion(bool more) override;

	void handle_request();

	on_complete_op send_next();
	void send_finish_marker();

	static void deserialize(const TRpcRequest &rpc_request, TRequest &request);

	TRpcResponse serialize_next(const TResponse &response, bool &complete);
	static fb::Offset<fb::Vector<uint8_t>> put_data_part(fb::grpc::MessageBuilder &builder,
	                                                     const ell::data_pointer &total_data,
	                                                     size_t &current_part_offset);

private:
	on_complete_op on_complete_op_;

	TRpcRequest rpc_request_;
	grpc::ServerAsyncWriter<TRpcResponse> rpc_call_io_;

	std::unique_ptr<TResponse> response_;
	ell::data_pointer data_;
	fb::grpc::MessageBuilder builder_;
	size_t response_json_offset_;
	size_t response_data_offset_;

	THandler handler_;
};

} // namespace grpc_dnet
