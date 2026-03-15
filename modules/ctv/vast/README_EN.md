# CTV VAST Module

The CTV VAST module provides VAST processing for Connected TV ad workflows in Prebid Server.

## Module Structure

```
modules/ctv/vast/
├── vast.go              # Main pipeline orchestration
├── handler.go           # HTTP handler scaffold
├── types.go             # Public types and interfaces
├── config.go            # Layered configuration merge and defaults
├── enrich/              # VAST enrichment logic
├── format/              # VAST XML formatting
├── model/               # VAST XML structures and parser helpers
└── select/              # Bid selection logic
```

## Pipeline Overview

The main `BuildVastFromBidResponse` flow performs these steps:

1. Select bids from the auction response.
2. Parse each bid's `AdM` as VAST, or build a skeleton when allowed.
3. Enrich the parsed ad with canonical metadata.
4. Format the final pod into VAST XML.

## Core Components

- `vast.go`: orchestration entrypoint and processor wrapper.
- `config.go`: PBS-style host/account/profile merge with runtime defaults.
- `select/`: price-first ranking with deal prioritization and pod limits.
- `enrich/`: pricing, advertiser, duration, category, and debug metadata injection.
- `format/`: final XML serialization with pod sequence handling.
- `model/`: VAST XML structures, no-ad helpers, skeleton builders, and parser utilities.

## Current Local Adaptation

This workspace ports the upstream implementation to `github.com/prebid/prebid-server/v4/...` imports and keeps the package standalone until runtime wiring is added elsewhere.