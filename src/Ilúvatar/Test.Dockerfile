FROM ubuntu:24.04

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		ca-certificates \
		iptables \
		openssl \
		pigz \
		xz-utils \
	; \
	rm -rf /var/lib/apt/lists/*

ENV DOCKER_TLS_CERTDIR=/certs
RUN mkdir /certs /certs/client && chmod 1777 /certs /certs/client

COPY ./docs/setup.sh .
RUN apt-get update && apt-get install sudo -y && \
    ln -snf /usr/share/zoneinfo/America/Indiana/Indianapolis /etc/localtime && \
    echo America/Indiana/Indianapolis > /etc/timezone && \
     ./setup.sh --worker --load


COPY --from=docker:28.2.2-dind /usr/local/bin/ /usr/local/bin/

#VOLUME /var/lib/docker

ENTRYPOINT ["dockerd-entrypoint.sh"]
