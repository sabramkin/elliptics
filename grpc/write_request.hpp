#include "requests_common.hpp"


namespace grpc_dnet {

class write_request_manager : public request_manager_base {
public:
	typedef ioremap::elliptics::dnet_write_request TRequest;
	typedef ioremap::elliptics::dnet_write_response TResponse;
	typedef std::function<void (std::shared_ptr<TRequest>)> TRequestProcessor;

public:
	write_request_manager(service_provider_info *service_provider_info_in, TRequestProcessor request_processor_in);

	void send_response(const TResponse &response);

private:
	typedef flatbuffers::grpc::Message<fb_grpc_dnet::WriteRequest> TRpcRequest;
	typedef flatbuffers::grpc::Message<fb_grpc_dnet::WriteResponse> TRpcResponse;

private:
	virtual void process_completion(bool more);

	void on_request_available_next();
	void on_request_final();

	void on_response_sent();

	static void append_data(data_pointer &total_data, size_t &current_part_offset, fb::Vector<uint8_t> *current_part);
	static void deserialize_next(const TRpcRequest &rpc_request, TRequest &request);
	static TRpcResponse serialize(const TResponse &response);

private:
	grpc::ServerAsyncResponseWriter<TRpcResponse> rpc_responder_;

	std::shared_ptr<ioremap::elliptics::dnet_write_request> request_; // TODO: unique_ptr
	size_t json_offset_;
	size_t data_offset_;

	TRequestProcessor request_processor_;
};

} // namespace grpc_dnet
