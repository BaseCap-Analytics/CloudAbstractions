# BaseCap Cloud Abstractions

## Build Status
![build-status](https://basecapanalytics.visualstudio.com/_apis/public/build/definitions/d14b2237-4f73-409d-af23-b13e5b1e4b8c/8/badge)

[NuGet Package](https://www.myget.org/feed/bca/package/nuget/BaseCap.CloudAbstractions)


## Summary
This repo contains code used to abstract away implementation details pertaining to the use of resources from a cloud provider (Azure, AWS, etc). This allows calling code to use interface-based contracts that can be mocked, rather than directly utilizing the resource code. This allows product code to be mocked for testing or local development instead of hitting remote resources.

## Package Organization

The package is organized into two different areas; abstractions and implementations. The abstractions are the contracts for what functionality is provided by the remote service. The implementations area are the concrete classes used to implement the communication with the remote service; this area is broken down into plaintext and encrypted implementations in order to allow seamless encryption and decryption based on a provided encryption key.

## License
This repository is licensed under the MIT license.
