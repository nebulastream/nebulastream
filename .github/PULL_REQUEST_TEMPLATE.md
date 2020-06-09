<!--
*Thank you very much for contributing to NebulaStream - we are happy that you want to help us improve NebulaStream. To help the NES team review your contribution in the best possible way, please go through the checklist below, which will get the contribution into a shape in which it can be best reviewed.*

*Please understand that we do not do this to make contributions to NES a hassle. In order to uphold a high standard of quality for code contributions, while at the same time managing a large number of contributions, we need contributors to prepare the contributions well, and give reviewers enough contextual information for the review. Please also understand that contributions that do not follow this guide will take longer to review and thus typically be picked up with lower priority by the NES team (and usually will be given to Steffen :D).*

## Contribution Checklist

  - Make sure that the pull request corresponds to a Github issues.
  
  - Name the pull request in the form "ISSUE: Title of the pull request", where *ISSUE* should be replaced by the actual issue number. 

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.
  
  - Make sure that the change passes the automated tests, i.e., `make test` passes. 

  - Each pull request should address only one issue, not mix up code from multiple issues.
  
  - Each commit in the pull request has a meaningful commit message (including the issue id)

  - Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.

-->

## Review Checklist
- use meaningfully comments and variable names
- no def in hpp => cpp (only for templates)
- always comment hpp (e.g. by copy from parent class) => one large over the class which describes the class in detail and for each function
- too few logging statements and thus I cannot comprehend what is going on from looking at the logs
  - Trace - Only when I would be "tracing" the code and trying to find one part of a function specifically.
  - Debug - Information that is diagnostically helpful to people more than just developers (IT, sysadmins, etc.).
  - Info - Generally useful information to log (service start/stop, configuration assumptions, etc). Info I want to always have available but usually don't care about under normal circumstances. This is my out-of-the-box config level.
  - Warn - Anything that can potentially cause application oddities, but for which I am automatically recovering. (Such as switching from a primary to backup server, retrying an operation, missing secondary data, etc.)
  - Error - Any error which is fatal to the operation, but not the service or application (can't open a required file, missing data, etc.). These errors will force user (administrator, or direct user) intervention. These are usually reserved (in my apps) for incorrect connection strings, missing services, etc.
  - Fatal - Any error that is forcing a shutdown of the service or application to prevent data loss (or further data loss). I reserve these only for the most heinous errors and situations where there is guaranteed to have been data corruption or loss
- no _
- remove commented code
## What is the purpose of the change

*(For example: This pull request makes task deployment go through the gRPC server, rather than through CAF. That way we avoid CAF's problems them on each deployment.)*


## Brief change log

*(for example:)**
  - *The TaskInfo is stored in the RPC message*
  - *Deployments RPC transmits the following information:....*
  - *NodeEngine does the following: ...*


## Verifying this change

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
  - *Added integration tests for end-to-end deployment with large payloads (100MB)*
  - *Extended integration test for recovery after master failure*
  - *Added test that validates that TaskInfo is transferred only once across recoveries*
  - *Manually verified the change by running a 4 node cluser with 2 Coordinators and 4 NodeEngines, a stateful streaming program, and killing one Coordinators and two NodeEngines during the execution, verifying that recovery happens correctly.*

## Does this pull request potentially affect one of the following parts:

  - Dependencies (does it add or upgrade a dependency): (yes / no)
  - The compiler: (yes / no / don't know)
  - The threading model: (yes / no / don't know)
  - The runtime per-record code paths (performance sensitive): (yes / no / don't know)
  - The network stack: (yes / no / don't know)
  - Anything that affects deployment or recovery: Coordinator (and its components), NodeEngine (and its components): (yes / no / don't know)

## Documentation

  - Does this pull request introduce a new feature? (yes / no)
  - If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)
