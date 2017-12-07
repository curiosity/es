===================================================================
es - functions for composing elasticsearch interactions with python
===================================================================

Introduction
============

This is a library that provides a set of functions to compose queries, filters,
aggregations, and other interactions for elasticsearch 1.0+. It depends on
elasticsearch-py (the official python elasticsearch client) for actually
executing operations against an elasticsearch index, but composing queries can
be used in a vacuum without access to either elasticsearch or the elasticsearch-py
library (if you say, want to use a different backend). 

This library is heavily inspired by the library elastisch for Clojure, and
provides a similar toolset to the Python programmer.

This library depends on yunobuiltin, which provides utility functions such as
merge_with to make working purely with python data structures (dict, list, etc)
easier.

Queries and Filters
===================

Aggregations
============

Index Operations
================

.. |copy| unicode:: U+000A9 .. COPYRIGHT SIGN

License (MIT/Expat)
====================

Copyright |copy| 2014, 2015, 2016, 2017 Beacon Solutions, Inc

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


