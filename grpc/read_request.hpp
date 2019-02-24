#include "requests_common.hpp"


namespace grpc_dnet {

class read_request_manager : public request_manager_base {
public:
	typedef ioremap::elliptics::dnet_read_request TRequest;
	typedef ioremap::elliptics::dnet_read_response TResponse;
	typedef std::function<void (std::shared_ptr<TRequest>)> THandler;

public:
	read_request_manager(service_provider_info *service_provider_info_in, THandler request_processor_in);

	void send_response(const std::shared_ptr<TResponse> &response);

private:
	enum class on_complete_op {
		HANDLE_REQUEST,
		HANDLE_SEND_PARTIAL_COMPLETE,
		HANDLE_SEND_ALL_COMPLETE,
		DELETE,
	};

	typedef fb::grpc::Message<fb_grpc_dnet::ReadRequest> TRpcRequest;
	typedef fb::grpc::Message<fb_grpc_dnet::ReadResponse> TRpcResponse;

private:
	virtual void process_completion(bool more) override;

	void handle_request();
	void send_next();

	static void deserialize(const TRpcRequest &rpc_request, TRequest &request);

	static TRpcResponse serialize_next(const TResponse &response, bool &complete);
	static fb::Offset<fb::Vector<uint8_t>> put_data_part(fb::grpc::MessageBuilder &builder,
	                                                     const data_pointer &total_data,
	                                                     size_t &current_part_offset);

private:
	on_complete_op on_complete_op_;

	TRpcRequest rpc_request_;
	grpc::ServerAsyncResponseWriter<TRpcResponse> rpc_responder_;

	std::shared_ptr<TResponse> response_; // TODO: unique_ptr
	size_t response_json_offset_;
	size_t response_data_offset_;

	THandler handler_;
};

} // namespace grpc_dnet
