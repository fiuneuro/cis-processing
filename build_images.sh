# chmod stuff
chmod 774 scripts/*.py
chmod 774 scripts/*.sh

docker build -t cis/bidsify:v0.0.1 .

# This converts the Docker image cis/bidsify to a singularity image,
# to be located in /Users/tsalo/Documents/singularity_images/
docker run --privileged -t --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /Users/tsalo/Documents/singularity_images:/output \
  singularityware/docker2singularity \
  -m "/scratch" \
  cis/bidsify:v0.0.1
