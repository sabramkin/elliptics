#pragma once

namespace ioremap { namespace elliptics {

template<typename T>
data_pointer serialize(const T &value);

template<typename T>
void deserialize(const data_pointer &data, T &value, size_t &offset);

template<typename T>
void deserialize(const data_pointer &data, T &value) {
	size_t offset = 0;
	deserialize(data, value, offset);
}

}} // namespace ioremap::elliptics
