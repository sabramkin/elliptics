/*
 * 2008+ Copyright (c) Evgeniy Polyakov <zbr@ioremap.net>
 * 2012+ Copyright (c) Ruslan Nigmatullin <euroelessar@yandex.ru>
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 */

#ifndef ELLIPTICS_RESULT_ENTRY_HPP
#define ELLIPTICS_RESULT_ENTRY_HPP

#include "elliptics/utils.hpp"
#include "elliptics/async_result.hpp"

#include <map>
#include <vector>

class n2_body;

namespace ioremap { namespace elliptics {

class callback_result_data_base;

class callback_result_entry
{
	public:
		callback_result_entry();
		callback_result_entry(const callback_result_entry &other);
		callback_result_entry(const std::shared_ptr<callback_result_data_base> &data);
		~callback_result_entry();

		callback_result_entry &operator =(const callback_result_entry &other);

		//! It's not a null reply, so it's safe to call other methods
		bool is_valid() const;
		//! It's reply with zero-status and empty body
		bool is_ack() const;
		//! It's last package at this transaction
		bool is_final() const;
		//! This package was generated by client
		bool is_client() const;
		//! Status of this reply, it's shortcut for command()->status
		int status() const;
		//! Error info for this package if exists
		error_info error() const;

		struct dnet_addr	*address() const;
		struct dnet_cmd		*command() const;

		// TODO(sabramkin): Deprecated methods, for usage with callback_result_data (old) only
		data_pointer		raw_data() const;
		data_pointer		data() const;
		uint64_t		size() const;
		template <typename T>
		inline T		*data() const
		{ return data().data<T>(); }

		// TODO(sabramkin): New methods, for usage with n2_callback_result_data (new) only
		n2_body *body() const;

		// TODO(sabramkin): Hint to determine what methods to use: new or deprecated
		bool tmp_is_n2_protocol() const;

	protected:
		std::shared_ptr<callback_result_data_base> m_data;
};

class read_result_entry : public callback_result_entry
{
	public:
		read_result_entry();
		read_result_entry(const read_result_entry &other);
		~read_result_entry();

		read_result_entry &operator =(const read_result_entry &other);

		struct dnet_io_attr *io_attribute() const;
		data_pointer file() const;
};

class lookup_result_entry : public callback_result_entry
{
	public:
		lookup_result_entry();
		lookup_result_entry(const lookup_result_entry &other);
		~lookup_result_entry();

		lookup_result_entry &operator =(const lookup_result_entry &other);

		struct dnet_addr *storage_address() const;
		struct dnet_file_info *file_info() const;
		const char *file_path() const;
};

class monitor_stat_result_entry : public callback_result_entry
{
	public:
		monitor_stat_result_entry();
		monitor_stat_result_entry(const monitor_stat_result_entry &other);
		~monitor_stat_result_entry();

		monitor_stat_result_entry &operator =(const monitor_stat_result_entry &other);

		std::string statistics() const;
};

class node_status_result_entry : public callback_result_entry
{
	public:
		node_status_result_entry();
		node_status_result_entry(const node_status_result_entry &other);
		~node_status_result_entry();

		node_status_result_entry &operator =(const node_status_result_entry &other);

		struct dnet_node_status *node_status() const;
};

class iterator_result_entry : public callback_result_entry
{
	public:
		iterator_result_entry();
		iterator_result_entry(const iterator_result_entry &other);
		~iterator_result_entry();

		iterator_result_entry &operator =(const iterator_result_entry &other);

		dnet_iterator_response *reply() const;
		data_pointer reply_data() const;

		uint64_t id() const;
};

class backend_status_result_entry : public callback_result_entry
{
	public:
		backend_status_result_entry();
		backend_status_result_entry(const backend_status_result_entry &other);
		~backend_status_result_entry();

		backend_status_result_entry &operator =(const backend_status_result_entry &other);

		dnet_backend_status_list *list() const;
		uint32_t count() const;
		dnet_backend_status *backend(uint32_t index) const;
};

typedef lookup_result_entry write_result_entry;
typedef callback_result_entry remove_result_entry;

typedef async_result<callback_result_entry> async_generic_result;
typedef std::vector<callback_result_entry> sync_generic_result;

typedef async_result<write_result_entry> async_write_result;
typedef std::vector<write_result_entry> sync_write_result;
typedef async_result<lookup_result_entry> async_lookup_result;
typedef std::vector<lookup_result_entry> sync_lookup_result;
typedef async_result<read_result_entry> async_read_result;
typedef std::vector<read_result_entry> sync_read_result;
typedef async_generic_result async_remove_result;
typedef sync_generic_result sync_remove_result;

typedef async_result<monitor_stat_result_entry> async_monitor_stat_result;
typedef std::vector<monitor_stat_result_entry> sync_monitor_stat_result;

typedef async_result<node_status_result_entry> async_node_status_result;
typedef std::vector<node_status_result_entry> sync_node_status_result;

typedef async_result<backend_status_result_entry> async_backend_control_result;
typedef std::vector<backend_status_result_entry> sync_backend_control_result;
typedef async_result<backend_status_result_entry> async_backend_status_result;
typedef std::vector<backend_status_result_entry> sync_backend_status_result;

typedef async_result<iterator_result_entry> async_iterator_result;
typedef std::vector<iterator_result_entry> sync_iterator_result;

static inline bool operator <(const dnet_raw_id &a, const dnet_raw_id &b)
{
	return dnet_id_cmp_str(a.id, b.id) < 0;
}

static inline bool operator ==(const dnet_raw_id &a, const dnet_raw_id &b)
{
	return dnet_id_cmp_str(a.id, b.id) == 0;
}

static inline bool operator ==(const ioremap::elliptics::data_pointer &a, const ioremap::elliptics::data_pointer &b)
{
	return a.size() == b.size() && memcmp(a.data(), b.data(), a.size()) == 0;
}

enum { skip_data = 0, compare_data = 1 };

template <int CompareData = compare_data>
struct dnet_raw_id_less_than {
	inline bool operator()(const dnet_raw_id &a, const dnet_raw_id &b) const {
		return dnet_id_cmp_str(a.id, b.id) < 0;
	}
};

}} /* namespace ioremap::elliptics */

#endif // ELLIPTICS_RESULT_ENTRY_HPP
