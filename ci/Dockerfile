FROM openjdk@sha256:ff7bfa31983e9b8be5e3853dafbe99a66937fe19bbcd7489bf5c205614e494ed

RUN apt-get update && apt-get install --no-install-recommends -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    software-properties-common \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -

RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/debian \
   $(lsb_release -cs) \
   stable"

RUN apt-get update && apt-get install --no-install-recommends -y \
    docker-ce \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/.docker \
 && echo "{}" > /root/.docker/config.json
