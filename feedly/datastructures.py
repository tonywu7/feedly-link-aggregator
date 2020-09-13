# MIT License
#
# Copyright (c) 2020 Tony Wu <tony[dot]wu(at)nyu[dot]edu>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import annotations

from collections.abc import (Hashable, MutableMapping, MutableSequence,
                             MutableSet)
from collections.abc import Set as SetCollection
from typing import Dict, Set, Tuple

Keywords = Set[Hashable]
KeywordCollection = Dict[Hashable, Hashable]


def compose_mappings(*mappings):
    base = {}
    base.update(mappings[0])
    for m in mappings[1:]:
        for k, v in m.items():
            if k in base and type(base[k]) is type(v):
                if isinstance(v, MutableMapping):
                    base[k] = compose_mappings(base[k], v)
                elif isinstance(v, MutableSet):
                    base[k] |= v
                elif isinstance(v, MutableSequence):
                    base[k].extend(v)
                else:
                    base[k] = v
            else:
                base[k] = v
    return base


class KeywordStore:
    def __init__(self):
        self._index: Dict[int, Hashable] = {}
        self._taggings: Dict[int, KeywordCollection] = {}

    def _get_hashes(self, **kws: Dict[Hashable, Hashable]) -> int:
        for hash_, keywords in self._taggings.items():
            match = True
            for category, keyword in kws.items():
                if category[0] == '_':
                    if category[1:] not in keywords:
                        match = False
                        break
                elif keyword not in keywords.get(category, {}):
                    match = False
                    break
            if match:
                yield hash_

    def all(self, **kws: Dict[Hashable, Hashable]) -> Hashable:
        for hash_ in self._get_hashes(**kws):
            yield self._index[hash_]

    def keywords(self, item):
        return self._taggings.get(hash(item), {})

    def items(self, **kws: Dict[Hashable, Hashable]) -> Tuple[Hashable, KeywordCollection]:
        for hash_ in self._get_hashes(**kws):
            yield self._index[hash_], self._taggings[hash_]

    def put(self, item: Hashable, **kws: KeywordCollection):
        hash_ = hash(item)
        self._index[hash_] = item
        taggings = self._taggings.setdefault(hash_, {})
        for category, kwset in kws.items():
            if not isinstance(kwset, SetCollection):
                kwset = {kwset}
            if category[0] == '_':
                raise ValueError('Keys that begin with _ are reserved')
            keywords = taggings.setdefault(category, set())
            keywords |= kwset

    def __len__(self) -> int:
        return len(self._index)

    def __and__(self, other: KeywordStore) -> KeywordStore:
        if not isinstance(other, KeywordStore):
            raise NotImplementedError()
        new = KeywordStore()
        common_keys = self._index.keys() & other._index.keys()
        taggings = {}
        for k in common_keys:
            this = self._taggings[k]
            that = other._taggings[k]
            tagging = {t: this[t] & that[t] for t in this.keys() & that.keys()}
            tagging = {k: v for k, v in tagging.items() if v}
            taggings[k] = tagging
        index = {k: self._index[k] for k in taggings}
        new._index = index
        new._taggings = taggings
        return new

    def __or__(self, other: KeywordStore) -> KeywordStore:
        if not isinstance(other, KeywordStore):
            raise NotImplementedError()
        new = KeywordStore()
        index = {**self._index, **other._index}
        taggings = {}
        for k in index:
            this = self._taggings.get(k, {})
            that = other._taggings.get(k, {})
            tagging = {t: this.get(t, set()) | that.get(t, set()) for t in this.keys() & that.keys()}
            tagging.update({t: this[t] for t in this.keys() - that.keys()})
            tagging.update({t: that[t] for t in that.keys() - this.keys()})
            taggings[k] = tagging
        new._index = index
        new._taggings = taggings
        return new

    def __sub__(self, other: KeywordStore) -> KeywordStore:
        if not isinstance(other, KeywordStore):
            raise NotImplementedError()
        new = KeywordStore()
        taggings = {}
        for k in self._index:
            this = self._taggings[k]
            that = other._taggings.get(k, {})
            tagging = {t: this[t] - that.get(t, set()) for t in this}
            tagging = {k: v for k, v in tagging.items() if v}
            taggings[k] = tagging
        index = {k: self._index[k] for k in taggings}
        new._index = index
        new._taggings = taggings
        return new

    def __str__(self):
        return str(self.for_json())

    def __repr__(self):
        return repr(self.for_json())

    def for_json(self):
        return {item: self._taggings[hash_] for hash_, item in self._index.items()}


def labeled_sequence(seq, key=True, zero_based=True, as_str=False):
    r = range(len(seq)) if zero_based else range(1, len(seq) + 1)
    if key:
        z = zip(r, seq)
    else:
        z = zip(seq, r)
    if as_str:
        return {str(k): v for k, v in z}
    return {k: v for k, v in z}
