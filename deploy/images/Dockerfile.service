# Runtime image for every Go binary. Binaries are built on the host with the pinned toolchain
# (CGO_ENABLED=0, see mk/30-images.mk), so the image is just distroless + one static file (~15 MB).
FROM gcr.io/distroless/static-debian13:nonroot
ARG BIN
COPY ${BIN} /app
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app"]
