// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Columns/ColumnVector.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/NaNUtils.h>
#include <Common/SipHash.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteHelpers.h>

#include <cmath>
#include <cstring>
#include <ext/bit_cast.h>

#if __SSE2__
#include <emmintrin.h>
#endif


namespace DB
{
namespace ErrorCodes
{
extern const int PARAMETER_OUT_OF_BOUND;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
} // namespace ErrorCodes


template <typename T>
StringRef ColumnVector<T>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin, const TiDB::TiDBCollatorPtr &, String &) const
{
    auto * pos = arena.allocContinue(sizeof(T), begin);
    memcpy(pos, &data[n], sizeof(T));
    return StringRef(pos, sizeof(T));
}

template <typename T>
void ColumnVector<T>::updateHashWithValue(size_t n, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    hash.update(data[n]);
}

template <typename T>
void ColumnVector<T>::updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr &, String &) const
{
    for (size_t i = 0, sz = size(); i < sz; ++i)
    {
        hash_values[i].update(data[i]);
    }
}

template <typename T>
void ColumnVector<T>::updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    auto s = data.size();

    if (hash.getData().size() != s)
        throw Exception(
            fmt::format("Size of WeakHash32 does not match size of column: column size is {}, hash size is {}", s, hash.getData().size()),
            ErrorCodes::LOGICAL_ERROR);

    const T * begin = data.data();
    const T * end = begin + s;
    UInt32 * hash_data = hash.getData().data();

    while (begin < end)
    {
        if constexpr (is_fit_register<T>)
            *hash_data = intHashCRC32(*begin, *hash_data);
        else
            *hash_data = wideIntHashCRC32(*begin, *hash_data);

        ++begin;
        ++hash_data;
    }
}

template <typename T>
struct ColumnVector<T>::less
{
    const Self & parent;
    int nan_direction_hint;
    less(const Self & parent_, int nan_direction_hint_)
        : parent(parent_)
        , nan_direction_hint(nan_direction_hint_)
    {}
    bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::less(parent.data[lhs], parent.data[rhs], nan_direction_hint); }
};

template <typename T>
struct ColumnVector<T>::greater
{
    const Self & parent;
    int nan_direction_hint;
    greater(const Self & parent_, int nan_direction_hint_)
        : parent(parent_)
        , nan_direction_hint(nan_direction_hint_)
    {}
    bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::greater(parent.data[lhs], parent.data[rhs], nan_direction_hint); }
};

template <typename T>
void ColumnVector<T>::getPermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    size_t s = data.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    if (limit >= s)
        limit = 0;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), greater(*this, nan_direction_hint));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), less(*this, nan_direction_hint));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), greater(*this, nan_direction_hint));
        else
            std::sort(res.begin(), res.end(), less(*this, nan_direction_hint));
    }
}

template <typename T>
const char * ColumnVector<T>::getFamilyName() const
{
    return TypeName<T>::get();
}

template <typename T>
MutableColumnPtr ColumnVector<T>::cloneResized(size_t size) const
{
    auto res = this->create();

    if (size > 0)
    {
        auto & new_col = static_cast<Self &>(*res);
        new_col.data.resize(size);

        size_t count = std::min(this->size(), size);
        memcpy(&new_col.data[0], &data[0], count * sizeof(data[0]));

        if (size > count)
            memset(&new_col.data[count], static_cast<int>(value_type()), size - count);
    }

    return res;
}

template <typename T>
UInt64 ColumnVector<T>::get64(size_t n) const
{
    return ext::bit_cast<UInt64>(data[n]);
}

template <typename T>
UInt64 ColumnVector<T>::getUInt(size_t n) const
{
    return static_cast<UInt64>(data[n]);
}

template <typename T>
Int64 ColumnVector<T>::getInt(size_t n) const
{
    return static_cast<Int64>(data[n]);
}

template <typename T>
void ColumnVector<T>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto & src_vec = static_cast<const ColumnVector &>(src);

    if (start + length > src_vec.data.size())
        throw Exception(
            fmt::format(
                "Parameters are out of bound in ColumnVector<T>::insertRangeFrom method, start={}, length={}, src.size()={}",
                start,
                length,
                src_vec.data.size()),
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = data.size();
    data.resize(old_size + length);
    memcpy(&data[old_size], &src_vec.data[start], length * sizeof(data[0]));
}

template <typename T>
ColumnPtr ColumnVector<T>::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t size = data.size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create();
    Container & res_data = res->getData();

    if (result_size_hint)
        res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    const UInt8 * filt_pos = &filt[0];
    const UInt8 * filt_end = filt_pos + size;
    const T * data_pos = &data[0];

#if __SSE2__
    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */

    static constexpr size_t SIMD_BYTES = 16;
    const __m128i zero16 = _mm_setzero_si128();
    const UInt8 * filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filt_pos < filt_end_sse)
    {
        int mask = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));

        if (0 == mask)
        {
            /// Nothing is inserted.
        }
        else if (0xFFFF == mask)
        {
            res_data.insert(data_pos, data_pos + SIMD_BYTES);
        }
        else
        {
            for (size_t i = 0; i < SIMD_BYTES; ++i)
                if (filt_pos[i])
                    res_data.push_back(data_pos[i]);
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }
#endif

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <typename T>
ColumnPtr ColumnVector<T>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    size_t size = data.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(limit);
    typename Self::Container & res_data = res->getData();
    for (size_t i = 0; i < limit; ++i)
        res_data[i] = data[perm[i]];

    return res;
}

template <typename T>
ColumnPtr ColumnVector<T>::replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const
{
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    assert(start_row < end_row);
    assert(end_row <= size);

    if (0 == size)
        return this->create();

    auto res = this->create();
    typename Self::Container & res_data = res->getData();

    res_data.reserve(offsets[end_row - 1]);

    IColumn::Offset prev_offset = 0;

    for (size_t i = start_row; i < end_row; ++i)
    {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            res_data.push_back(data[i]);
        }
    }

    return res;
}

template <typename T>
void ColumnVector<T>::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

template <typename T>
void ColumnVector<T>::getExtremes(Field & min, Field & max) const
{
    size_t size = data.size();

    if (size == 0)
    {
        min = static_cast<typename NearestFieldType<T>::Type>(0);
        max = static_cast<typename NearestFieldType<T>::Type>(0);
        return;
    }

    bool has_value = false;

    /** Skip all NaNs in extremes calculation.
        * If all values are NaNs, then return NaN.
        * NOTE: There exist many different NaNs.
        * Different NaN could be returned: not bit-exact value as one of NaNs from column.
        */

    T cur_min = NaNOrZero<T>();
    T cur_max = NaNOrZero<T>();

    for (const T x : data)
    {
        if (isNaN(x))
            continue;

        if (!has_value)
        {
            cur_min = x;
            cur_max = x;
            has_value = true;
            continue;
        }

        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    min = static_cast<typename NearestFieldType<T>::Type>(cur_min);
    max = static_cast<typename NearestFieldType<T>::Type>(cur_max);
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ColumnVector<UInt8>;
template class ColumnVector<UInt16>;
template class ColumnVector<UInt32>;
template class ColumnVector<UInt64>;
template class ColumnVector<UInt128>;
template class ColumnVector<Int8>;
template class ColumnVector<Int16>;
template class ColumnVector<Int32>;
template class ColumnVector<Int64>;
template class ColumnVector<Float32>;
template class ColumnVector<Float64>;
} // namespace DB
