#pragma once

#include "requests_common.hpp"

namespace grpc_dnet {

class write_request_manager : public request_manager_base {
public:
	using TRequest = ell::dnet_write_request;
	using TResponse = ell::dnet_lookup_response;
	using THandler = std::function<void (std::unique_ptr<TRequest>)>;

public:
	write_request_manager(service_provider_info *service_provider_info_in, THandler request_processor_in);

	void send_response(std::unique_ptr<TResponse> response);

private:
	enum class on_complete_op {
		READ_REQUEST_FIRST,
		READ_REQUEST,
		DELETE,
	};

	using TRpcRequest = fb::grpc::Message<fb_grpc_dnet::WriteRequest>;
	using TRpcResponse = fb::grpc::Message<fb_grpc_dnet::LookupResponse>;

private:
	virtual void process_completion(bool more) override;

	void read_next();
	void handle_final();

	void deserialize_next(const TRpcRequest &rpc_request, TRequest &request);
	static void append_data(ell::data_pointer &total_data, size_t &current_part_offset,
	                        const fb::Vector<uint8_t> *current_part);

	static TRpcResponse serialize(const TResponse &response);

private:
	on_complete_op on_complete_op_;

	grpc::ServerAsyncReader<TRpcResponse, TRpcRequest> rpc_call_io_;

	std::unique_ptr<TRequest> request_;
	size_t request_json_offset_;
	size_t request_data_offset_;

	THandler handler_;
};

} // namespace grpc_dnet
