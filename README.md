[![Build status](https://ci.appveyor.com/api/projects/status/s2jxir4ghx8kui53/branch/master?svg=true)](https://ci.appveyor.com/project/SteveHorsfield/chimayo/branch/master)

# chimayo

Chimayo is a library for creating and manipulating SQL Server Integration Services packages with F# code.

Chimayo provides a completely different development experience for producing SSIS packages that fits
with modern software development:

* Write code, not a custom DSL, and benefit from refactoring, compile-time testing, test automation and reuse
* Generate SSIS packages that work well in version control systems, such as Git, or just keep the F#
  code and generate them on demand
* Benefit from functional programming with pattern matching, immutable data structures and strong typing

Chimayo is not object-oriented software and is designed to be consumed directly from F#.

Chimayo also provides a command line tool that can be used to convert between supported SSIS formats.

# License

This code is released under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).

See [LICENSE] (https://github.com/chimayo/chimayo/blob/master/LICENSE).

You must also include [NOTICE](https://github.com/chimayo/chimayo/blob/master/NOTICE).

# Getting started

NuGet packages are coming. For the moment, you need to download and compile the source.

I am currently in the process of setting up the project for community involvement. Please be patient.

# Roadmap and development tracking

The following roadmap is entirely provisional, and I welcome suggestions from the community.

---
	0.1	Initial release, NuGet packages, basic documentation
	0.2	Automated regression tests
	0.3	Extraction of XML library to separate project
	0.4	Extraction of code generation library to separate project
	0.5	Support for SQL Server 2014 and 2016 packages
---

All work is tracked in GitHub issues.

# Links

* Main website [https://chimayo.github.io](https://chimayo.github.io)
* Twitter [@ChimayoProject](https://twitter.com/chimayoproject)


