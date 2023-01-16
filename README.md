## _<Chain>_ on StreamingFast Tools Library

All the _<Chain>_ on StreamingFast top-level binary like [firehose-ethereum](https://github.com/streamingfast/firehose-ethereum), [firehose-near](https://github.com/streamingfast/firehose-near) and all others have a bunch of tools to help maintain and debug the various elements of the stack.

This repository contains a chain agnostic shared library that is used to avoid duplication across all projects. It never has chain specific code and everything that is chain specific is injected into the appropriate function.

By conventions, all the functions exported by this library related to specific tooling assumes full control of the console output. It pretty prints elements. This greatly reduce the complexity and the specific project can share as much code as possible.
