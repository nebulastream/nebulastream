# Nightly CI

TL;DR: The nightly owner is accountable. Responsibility for fixing is assigned to the team member that either owns it or probably broke it.

NOTE: We use the terms "Responsible", "Accountable", and "Role" as defined in the [RACI model](https://en.wikipedia.org/wiki/Responsibility_assignment_matrix).

If the PR CI fails, the PR creator is (implicitly) responsible for fixing it.
The PR reviewers are (implicitly) accountable that the PR is only merged given PR CI succeeds.

If Nightly CI fails, there are neither PR owner nor PR reviewers, thus we are explicit about responsibility and accountability.

But first, a rough categorization of the jobs run during nigthly CI:

- **regression surfacers**: Tests like e.g. the [other CMake config tests](https://github.com/nebulastream/nebulastream/blob/7a2e51daa854828186f9d4be926b295ce974a4c7/.github/workflows/nightly.yml#L75-L114).
  In a perfect world, these tests would be run as part of PR CI so that the codebase adheres to the [no rocket science rule](https://matklad.github.io/2024/03/22/basic-things.html#Not-Rocket-Science-Rule).
  They are run in nightly as a compromise to reduce the running time of the PR CI.
- **automation**: Generates some result per run (i.e. benchmark result, clang-tidy report, published docker image).
  Needs fixing if broken to ensure the availability of the result.

To ensure that broken automation and surfaced regressions are fixed, we have the following roles:

- **nightly owner** 🤔: Generally accountable, i.e. ensures that broken automations and surfaced regressions are fixed in a timely manner.
  Watches over automation owner and dispatcher.
  Also accountable & responsible for evolving this process (and its documentation)
  and considering which regression surfacers could be integrated into PR CI (or moved from PR CI to nightly CI).
- **automation owner** 🧑🔧: Responsible that the automation produces its result.
  Maintains the automation. Fixes it, when it breaks.
- **dispatcher** 🔭: Responsible to assign the responsibility for fixing the regressions.
  Is notified when regression surfaces.
  Does initial assessment to determine who will be the one to investigate & fix it.
- **investigator** 🕵️: Responsible to fix bug

Note that not all of these roles have to be performed by distinct team members.
Roles can be assigned in a flexible manner.

When the nightly CI fails, a message is sent into the `#ci` channel
mentioning the owner and dispatcher explicitly.
	
### Example 1

```
$nightly owner: Hey @dispatcher, test_xy failed last night.
$dispatcher: Yeah, a failed invariant in the compiler. I assigned this to
             @team_member_2 since his PR was merged yesterday and changed
             some stuff there. We briefly discussed and don't think a
             workaround is necessary.
             @team_member_2 will take some time tomorrow afternoon to 
             debug this. Should be done by friday.
< friday, 10am >
$nigthly owner: Hey @ dispatcher, test_xy was still failing last night.
$dispatcher: @team_member_2 has a patch: !123 which is currently in review.
             I'll remind the reviewers. !123 should be merged by monday.
```
