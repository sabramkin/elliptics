// Pointers on that are stored in queue
class BaseMsgHolder {
public:
	// calls corresponding handler
	virtual void process() = 0;

	// depends on message type and transport type
	virtual void send() = 0;
};

class LookupRequest; // deserialized message

class LookupRequestHolderBase : public BaseMsgHolder {
public:
	// called async
	virtual void process() override {
		LookupRequest msg;
		restore(msg);
		dnet_process_lookup(msg)
	};

	virtual void save(const Msg &msg) = 0; // hide dependency on transport type
	virtual void restore(Msg &msg) = 0; // hide dependency on transport type

	ptr<LookupResponseHolderBase> create_response_holder();
}

class NativeLookupRequestHolder : public BaseMsgHolder {
public:
	virtual void save(const LookupRequest &msg) override {
		// or 1. LookupRequest -> msgpack_blob + tail
		// or 2. simply save LookupRequest
	}

	virtual void restore(LookupRequest &msg) override {
		//
	}

	virtual void send() override {
		// or 1. msgpack_blob + tail -> send to socket (same code for all messages)
		// or 2. LookupRequest -> msgpack_blob + tail -> send to socket
	}
}

class GrpcLookupRequestHolder : public BaseMsgHolder {
public:
	virtual void save(const LookupRequest &msg) override {
		// or 1. LookupRequest -> array of grpc::Message<fb::LookupRequest>
		// or 2. simply save LookupRequest
	}

	virtual void restore(LookupRequest &msg) override {
		//
	}

	virtual void send() override {
		// or 1. array of grpc::Message<fb::LookupRequest> -> streaming send grpc
		// or 2. LookupRequest -> array of grpc::Message<fb::LookupRequest> -> streaming send grpc
	}
}
