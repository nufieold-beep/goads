# CTV VAST Enrichment Module

This PBS module wraps the local `modules/ctv/vast` pipeline and applies VAST enrichment during the `raw_bidder_response` stage.

## Host Config

```json
{
  "hooks": {
    "modules": {
      "prebid": {
        "ctv_vast_enrichment": {
          "enabled": true,
          "default_currency": "USD",
          "allow_skeleton_vast": false,
          "debug": false
        }
      }
    }
  }
}
```

## Execution Plan

```json
{
  "hooks": {
    "host_execution_plan": {
      "endpoints": {
        "/openrtb2/auction": {
          "stages": {
            "raw_bidder_response": {
              "groups": [
                {
                  "timeout": 50,
                  "hook_sequence": [
                    {
                      "module_code": "prebid.ctv_vast_enrichment",
                      "hook_impl_code": "HandleRawBidderResponseHook"
                    }
                  ]
                }
              ]
            }
          }
        }
      }
    }
  }
}
```

Account-level config can override the same module fields under `account.hooks.modules.prebid.ctv_vast_enrichment`.