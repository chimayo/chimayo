Introduction
============

Chimayo SSIS was conceived as a way to reduce the complexity
of a hundred SSIS packages operating and being maintained for
a single product.

I had looked at the market and didn't find anything that really
met the goals I had in mind:

* No more XML in source control. Too many issues merging changes
* Programmatic generation of content and support for higher-level constructs
  * Refactoring
  * Reuse
  * Regression testing
  * Higher-level coding: parallelism, object graphs
  * Translation between SSIS 2008 R2, SSIS 2012 and SSIS 2016 (output in 
		SSIS 2012 or 2016 only)

Why not use BIML?
-----------------

BIML, BIMLScript and the BIML API are robust offerings for SSIS development,
but they weren't a good fit for my needs. I was looking for a much richer
development experience than working with XML and ASP/PHP-style scripting.
I wanted a functional-first experience with immutable data types, pattern
matching and compile-time syntax checking.

The BIML API gives a bit more in this direction, but without immutable data
structures and with an object-oriented approach, it wasn't what I was after.

If BIML, BIMLScript or the BIML API work for you, then don't change to Chimayo.
They are mature, well-supported and cover far more features than this project
is likely to ever support.

Why F#?
-------

Functional programming languages excel at data transformation, and F# is a
great functional programming language. Code in F# is generally much more
readable than in other .NET languages, supports higher level constructs much
more simply and has direct support for higher-order processing with pattern
matching, discriminated unions, type inference and function composition.

Why Chimayo?
------------

Chimayo links two of my likes: spices and a local Belgian beer. What better reason
could there be?



