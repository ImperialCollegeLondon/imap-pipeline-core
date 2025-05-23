---
name: 'Release Checklist IMAP'
about: Release Checklist for IMAP Data Pipeline
title: ''
labels: documentation
assignees: ''
---

- [ ] Merge to main, create release
- [ ] Deployed to test, verified it works
- [ ] Needs platform change?
- [ ] Create new ENV vars in platform deploy
- [ ] Deploy platform to test and test/verify
- [ ] Get manager approval
- [ ] Wait for jobs in prod to finish and disable the worker
- [ ] Note version running in prod for platform and IMAP
- [ ] Deploy platform update (if plat has changed)
- [ ] Deploy IMAP
- [ ] Check schedules are correct and as expected
- [ ] Any old prefect deployments need to be deleted?
- [ ] Populate new blocks and variables in prefect in PROD
- [ ] Re-enable the worker
- [ ] Smoke test - check new functionality works
- [ ] Check all scheduled jobs are still there
- [ ] Catch up any jobs/backfill any data
- [ ] Update release notes with a link to the GitHub actions deployment
- [ ] Either DONE and email everyone or rollback if needed

## Rollback/roll forwards plan

Trivial bugs like a deployment variable typo (and are untestable) may be quicker to fix with a "roll forwards" quick fixup release, so can be considered, but anything more substantive should be rolled back and fix in another release. The rollback plan is:

- [ ] Wait for jobs in prod to finish and disable the worker
- [ ] Deploy previous platform version (if required)
- [ ] Deploy previous IMAP version to prod
- [ ] Delete any “new” deployments in prefect, new blocks, new variables, if they were created by bad release
- [ ] Re-enable the worker
- [ ] Verify website jobs still run
- [ ] Notify it was a failure
