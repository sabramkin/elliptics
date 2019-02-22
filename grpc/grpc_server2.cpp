#include <thread>
#include <utility>
#include <memory>

#include <blackhole/attribute.hpp>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "elliptics.grpc.fb.h"

#include "../library/elliptics.h"
#include "../library/logger.hpp"

namespace fb = flatbuffers;

namespace grpc_dnet {

struct service_provider_info {
	dnet_node *node_;
	ell_grpc::Elliptics::AsyncService *service_;
	grpc::ServerCompletionQueue *cq_;
};


class request_manager_base {
public:
	request_manager_base(service_provider_info *service_provider_info__)
		: service_provider_info_(service_provider_info__)
		, process_request_(true)
		, processing_started_(false)
	{}

	virtual void process_completion(bool more) = 0;
	virtual ~request_manager_base() = default;

protected:
	service_provider_info *service_provider_info_;
	grpc::ServerContext ctx_;
	bool process_request_;
	bool processing_started_;
};


void deserialize_cmd(const fb_grpc_dnet::Cmd *fb_cmd, ioremap::elliptics::dnet_cmd_native &cmd) {
	auto fb_cmd_id = fb_cmd->id();
	auto fb_cmd_id_id = fb_cmd_id->id();
	auto id_len = fb::VectorLength(fb_cmd_id_id);
	if (id_len) {
		if (id_len != DNET_ID_SIZE) {
			throw std::invalid_argument("Unexpected cmd.id size");
		}

		memcpy(cmd.id.id.data(), fb_cmd_id->id());
	}

	cmd.id.group_id = fb_cmd_id->group_id();

	cmd.status = fb_cmd->status();
	cmd.cmd = fb_cmd->cmd();
	cmd.backend_id = fb_cmd->backend_id();
	cmd.trace_id = fb_cmd->trace_id();
	cmd.flags = fb_cmd->flags();
	cmd.trans = fb_cmd->trans();
	cmd.size = fb_cmd->size();
}


void nanosec_to_dnet_time(uint64_t fb_time, ioremap::elliptics::dnet_time_native &time) {
	time.tsec = fb_time / 1000000000;
	time.tnsec = fb_time % 1000000000;
}


uint64_t dnet_time_to_ns(const ioremap::elliptics::dnet_time_native &time) {
	return time.tsec * 1000000000 + time.tnsec;
}


void time_point_to_dnet_time(const std::chrono::system_clock::time_point &time_point, ioremap::elliptics::dnet_time_native &time) {
	uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time_point.time_since_epoch()).count();
	nanosec_to_dnet_time(ns, time);
}


class write_request_manager : public async_action {
public:
	typedef ioremap::elliptics::dnet_write_request TRequest;
	typedef ioremap::elliptics::dnet_write_response TResponse;

public:
	write_request_manager(
		service_provider_info *service_provider_info_in,
		std::function<void (std::shared_ptr<ioremap::elliptics::dnet_write_request>)> request_processor_in,
		)
		: async_action(service_provider_info_in)
		, request_(std::make_shared<TRequest>())
		, json_offset_(0)
		, data_offset_(0)
	{
		service_->RequestWrite(ctx_.get(), &rpc_responder_, cq_, cq_, this);
	}

	virtual void process_completion(bool more) override {
		if (!processing_started_) {
			processing_started_ = true;
			new write_request_processor(service_provider_info_);
		}

		if (!more) {
			process_request_collected();
			process_request_ = false;
		}

		if (process_request_) {
			process_request_next();
		} else {
			process_response();
		}
	}

	void send_response(const TResponse &response) {
		responder_.Finish(serialize(response), Status::OK, this);
	}

private:
	typedef flatbuffers::grpc::Message<fb_grpc_dnet::WriteRequest> TRpcRequest;
	typedef flatbuffers::grpc::Message<fb_grpc_dnet::WriteResponse> TRpcResponse;

private:
	void process_request_next() { // TODO: maybe do it async
		TRpcRequest rpc_request;
		responder_.Read(&rpc_request, this);
		deserialize_next(rpc_request, *request_);
	}

	void process_request_collected() { // TODO: maybe do it async
		if (json_offset_ != request.json.size() || data_offset_ != request.data.size()) {
			throw std::invalid_argument("Incomplete buffer");
		}

		time_point_to_dnet_time(ctx_.deadline(), request_.deadline);

		// TODO: enqueue request
	}

	void process_response() {
		delete this;
	}

	void append_data(data_pointer &total_data, size_t &current_part_offset, fb::Vector<uint8_t> *current_part) {
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

	static void deserialize_next(const TRpcRequest &rpc_request, TRequest &request) {
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

	static TRpcResponse serialize(const TResponse &response) {
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

private:
	std::shared_ptr<ioremap::elliptics::dnet_write_request> request_; // TODO: unique_ptr
	size_t json_offset_;
	size_t data_offset_;

	grpc::ServerAsyncResponseWriter<TRpcResponse> rpc_responder_;
};


struct call_processor {
	class accept_request : public async_op {
		accept_request(state *call_state)
			: call_state_(call_state)
		{

			service_->RequestLookup(&ctx_, &request_, &responder_, cq_, cq_, this);
		}

		virtual void done() override {

		}

	private:
		state *call_state_;
	};

	class send_response : public async_op {
		virtual void done() override {

		}

	private:
		state *call_state_;
	};

	struct state {
		service_provider_info *service_provider_info_;

		ServerContext ctx_;

		flatbuffers::grpc::Message<ell_grpc::LookupRequest> request_;
		flatbuffers::grpc::MessageBuilder mb_;

		ServerAsyncResponseWriter<flatbuffers::grpc::Message<ell_grpc::LookupResponse>> responder_;
	};
};


class CallData {
public:
	CallData(dnet_node *node, ell_grpc::Elliptics::AsyncService *service, ServerCompletionQueue *cq)
		: node_(node)
		, service_(service)
		, cq_(cq)
		, responder_(&ctx_)
		, status_(CREATE) {
		proceed();
	}

	void proceed() {
		switch (status_) {
		case CREATE: {
		        DNET_LOG_INFO(node_, "GRPC: create rpc: {:p}", (void*)this);
			status_ = PROCESS;
			service_->RequestLookup(&ctx_, &request_, &responder_, cq_, cq_, this);
			break;
		}
		case PROCESS: {
			new CallData(node_, service_, cq_);

			DNET_LOG_INFO(node_, "GRPC: process rpc: {:p}, peer: {}, message: {}", (void*)this, ctx_.peer(),
                                      request_.GetRoot()->key()->str());

			std::stringstream ss;
                        const auto metadata = ctx_.client_metadata();
                        for (const auto &pair: metadata) {
                                ss << "\t" << pair.first << " = " << pair.second << std::endl;
                        }
			DNET_LOG_INFO(node_, "GRPC: process rpc: {:p}, metadata:\n{}", (void*)this, ss.str());

			auto msg_offset = mb_.CreateString("Data for " + request_.GetRoot()->key()->str());
			auto lookup_offset = ell_grpc::CreateLookupResponse(mb_, msg_offset);
			mb_.Finish(lookup_offset);

			status_ = FINISH;
			responder_.Finish(mb_.ReleaseMessage<ell_grpc::LookupResponse>(), Status::OK, this);
			break;
		}
		case FINISH: {
		        DNET_LOG_INFO(node_, "GRPC: finish rpc: {:p}", (void*)this);
			delete this;
			break;
		}
		}
	}

private:
	dnet_node *node_;
	ell_grpc::Elliptics::AsyncService *service_;
	ServerCompletionQueue *cq_;
	ServerContext ctx_;

	flatbuffers::grpc::Message<ell_grpc::LookupRequest> request_;
	flatbuffers::grpc::MessageBuilder mb_;

	ServerAsyncResponseWriter<flatbuffers::grpc::Message<ell_grpc::LookupResponse>> responder_;

	enum CallStatus { CREATE, PROCESS, FINISH };
	CallStatus status_;
};

} // namespace grpc

struct dnet_grpc_server {
public:
	dnet_grpc_server(dnet_node *node)
	: node_(node) {
	}

	~dnet_grpc_server() {
		server_->Shutdown();

		cq_->Shutdown();

		thread_->join();
	}

	void run() {
		std::string server_address("0.0.0.0:2025");

		grpc::ServerBuilder builder;
		builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		builder.RegisterService(&service_);

		cq_ = builder.AddCompletionQueue();

		server_ = builder.BuildAndStart();

		DNET_LOG_INFO(node_, "GRPC: server listening on {}", server_address);

		thread_.reset(new std::thread([&] {
			new grpc::CallData(node_, &service_, cq_.get());
			void *tag;
			bool ok;
			while (cq_->Next(&tag, &ok) && ok) {
				static_cast<grpc::CallData*>(tag)->proceed();
			}
		}));
	}

private:
	dnet_node *node_;
	std::unique_ptr<grpc::ServerCompletionQueue> cq_;
	ell_grpc::Elliptics::AsyncService service_;
	std::unique_ptr<grpc::Server> server_;
	std::unique_ptr<std::thread> thread_;
};

void dnet_start_grpc_server(struct dnet_node *n) {
	n->io->grpc = new dnet_grpc_server(n);
	n->io->grpc->run();
}

void dnet_stop_grpc_server(struct dnet_node *n) {
	delete std::exchange(n->io->grpc, nullptr);
}

