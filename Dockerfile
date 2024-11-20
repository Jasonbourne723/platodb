# Step 1: Use an official Go image as the builder
FROM golang:alpine as builder
ENV GOPROXY https://goproxy.cn,direct
ENV GO111MODULE on
ENV GOOS=linux
ENV CGO_ENABLED=0
WORKDIR /go/cache
ADD go.mod .
ADD go.sum .
RUN go mod download
WORKDIR /app
ADD . .
COPY ./cmd/server/config.yaml ./config.yaml
# Build the Go application
RUN go build -o  server /app/cmd/server

# Step 2: Use a minimal image for running the built application
FROM golang:alpine as final


# Set working directory inside the runtime container
WORKDIR /app

# Copy the built executable from the builder
COPY --from=builder /app .

# Expose the port the server listens on (adjust as needed)
EXPOSE 6399

# Define the default command to run the application
CMD ["./server"]
