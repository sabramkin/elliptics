#include "requests_common.hpp"


namespace grpc_dnet {

class write_request_manager : public request_manager_base {
public:
	typedef ioremap::elliptics::dnet_write_request TRequest;
	typedef ioremap::elliptics::dnet_write_response TResponse;
	typedef std::function<void (std::shared_ptr<TRequest>)> THandler;

public:
	write_request_manager(service_provider_info *service_provider_info_in, THandler request_processor_in);

	void send_response(const std::shared_ptr<TResponse> &response);

private:
	enum class on_complete_op {
		READ_REQUEST_FIRST,
		READ_REQUEST,
		DELETE,
	};

	typedef fb::grpc::Message<fb_grpc_dnet::WriteRequest> TRpcRequest;
	typedef fb::grpc::Message<fb_grpc_dnet::LookupResponse> TRpcResponse;

private:
	virtual void process_completion(bool more) override;

	void read_next();
	void handle_final();

	static void deserialize_next(const TRpcRequest &rpc_request, TRequest &request);
	static void append_data(data_pointer &total_data, size_t &current_part_offset, fb::Vector<uint8_t> *current_part);

	static TRpcResponse serialize(const TResponse &response);

private:
	on_complete_op on_complete_op_;

	grpc::ServerAsyncResponseWriter<TRpcResponse> rpc_responder_;

	std::shared_ptr<TRequest> request_; // TODO: unique_ptr
	size_t request_json_offset_;
	size_t request_data_offset_;

	THandler handler_;
};

} // namespace grpc_dnet
