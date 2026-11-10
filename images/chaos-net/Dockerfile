# Network fault helper, injected as an ephemeral container (NET_ADMIN) by the controlboard.
FROM alpine:3.22
RUN apk add --no-cache iptables iproute2 iproute2-tc
COPY chaos-net.sh /usr/local/bin/chaos-net
RUN chmod 0755 /usr/local/bin/chaos-net
ENTRYPOINT ["/usr/local/bin/chaos-net"]
