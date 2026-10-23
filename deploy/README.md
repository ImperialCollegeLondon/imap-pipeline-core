# Build and run

## Weekly Docker refresh

CI rebuilds both Docker images every Monday at 03:17 UTC using `main` and the
three most recent Git tags, ordered by Git creator date (the tagger date for
annotated tags, or the commit date for lightweight tags). The workflow on the
default branch checks out each selected ref, so older releases do not need to
contain the weekly build configuration.

Scheduled builds pull fresh base images and bypass the layer cache. They update
the `main` and `matlab-main` image tags, the corresponding `latest` tags, and the
selected release version tags. Only the `main` rebuild updates `latest`, so older
releases cannot overwrite it. Scheduled builds do not run the test/QA jobs,
upload Python artefacts, or republish GitHub releases. Push and pull request
builds keep their existing behaviour.

## Local builds

Example matlab with docker can be built with:

`docker build -f deploy/MATLAB-Dockerfile --build-arg USERID=1000 -t testing .`

and run with

`docker run -v $(pwd)/tests/test_data:/data -e MLM_LICENSE_FILE={MLM_LICENSE_FILE} testing`

to create a CDF in `tests/test_data`
