#include "requests_common.hpp"

#include "read_request.hpp"
#include "write_request.hpp"

namespace fb = flatbuffers;

namespace grpc_dnet {

struct server {
public:
	server(dnet_node *node)
		: node_(node)
	{}

	~server() {
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

		thread_.reset(new std::thread(std::bind(&server::completion_thread, this)));
	}

	void completion_thread() {
		new read_request_manager(node_, &service_, cq_.get());
		new write_request_manager(node_, &service_, cq_.get());

		void *tag;
		bool ok;
		while (cq_->Next(&tag, &ok)) {
			static_cast<request_manager_base*>(tag)->proceed(ok);
		}
	}

private:
	dnet_node *node_;
	std::unique_ptr<grpc::ServerCompletionQueue> cq_;
	ell_grpc::Elliptics::AsyncService service_;
	std::unique_ptr<grpc::Server> server_;
	std::unique_ptr<std::thread> thread_;
};


void dnet_start_grpc_server(struct dnet_node *n) {
	n->io->grpc = new server(n);
	n->io->grpc->run();
}


void dnet_stop_grpc_server(struct dnet_node *n) {
	delete std::exchange(n->io->grpc, nullptr);
}

} // namespace grpc_dnet
