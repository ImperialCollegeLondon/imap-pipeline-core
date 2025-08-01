# Build and run

Example matlab with docker can be built with:

`docker build -f deploy/MATLAB-Dockerfile --build-arg USERID=1000 -t testing .`

and run with

`docker run -v $(pwd)/tests/test_data:/data -e MLM_LICENSE_FILE={MLM_LICENSE_FILE} testing`

to create a CDF in `tests/test_data`
