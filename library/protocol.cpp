#include "protocol.hpp"

#include "rapidjson/document.h"

namespace ioremap { namespace elliptics {

dnet_iterator_request::dnet_iterator_request()
	: iterator_id{0}
	, action{DNET_ITERATOR_ACTION_START}
	, type{DNET_ITYPE_NETWORK}
	, flags{0}
	, key_ranges{}
	, time_range{dnet_time{0, 0}, dnet_time{0, 0}}
	, groups{} {
}

dnet_iterator_request::dnet_iterator_request(uint32_t type, uint64_t flags,
                                             const std::vector<dnet_iterator_range> &key_ranges,
                                             const std::tuple<dnet_time, dnet_time> &time_range)
	: iterator_id{0}
	, action{DNET_ITERATOR_ACTION_START}
	, type{type}
	, flags{flags}
	, key_ranges{key_ranges}
	, time_range(time_range)
	, groups{} {
}

dnet_bulk_remove_request::dnet_bulk_remove_request() {}

dnet_bulk_remove_request::dnet_bulk_remove_request(const std::vector<dnet_id> &keys_in)
	: keys(keys_in) {}

dnet_bulk_remove_request::dnet_bulk_remove_request(const std::vector<std::pair<dnet_id, dnet_time>> &keys_in) {
	ioflags = DNET_IO_FLAGS_CAS_TIMESTAMP;
	keys.reserve(keys_in.size());
	timestamps.reserve(keys_in.size());
	for (auto &key : keys_in) {
		keys.push_back(key.first);
		timestamps.push_back(key.second);
	}
}

bool dnet_bulk_remove_request::is_valid() const {
	return (ioflags & DNET_IO_FLAGS_CAS_TIMESTAMP && (keys.size() == timestamps.size())) ||
		(!(ioflags & DNET_IO_FLAGS_CAS_TIMESTAMP) && (timestamps.size() == 0));
}

void validate_json(const std::string &json) {
	if (json.empty())
		return;

	rapidjson::Document doc;
	doc.Parse<0>(json.c_str());

	if (doc.HasParseError() || !doc.IsObject()) {
		throw std::runtime_error(doc.GetParseError());
	}
}

}} // namespace ioremap::elliptics
