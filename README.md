# BaseCap Cloud Abstractions

## Build Status

[![Build Status](https://dev.azure.com/basecapanalytics/Cloud%20Abstrations/_apis/build/status/CI?branchName=master)](https://dev.azure.com/basecapanalytics/Cloud%20Abstrations/_build/latest?definitionId=47&branchName=master)

[NuGet Package](https://www.myget.org/feed/bca/package/nuget/BaseCap.CloudAbstractions)

## Summary

This repo contains code used to abstract away implementation details pertaining to the use of resources from a cloud provider (Azure, AWS, etc). This allows calling code to use interface-based contracts that can be mocked, rather than directly utilizing the resource code. This allows product code to be mocked for testing or local development instead of hitting remote resources.

## Package Organization

The package is organized into two different areas; abstractions and implementations. The abstractions are the contracts for what functionality is provided by the remote service. The implementations area are the concrete classes used to implement the communication with the remote service; this area is broken down into plaintext and encrypted implementations in order to allow seamless encryption and decryption based on a provided encryption key.

## License

This repository is licensed under the MIT license.
