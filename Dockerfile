# Stage 1: Build the Go binaries
FROM golang:1.26.4-alpine AS builder

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Compile static Go server binary and migrate binary (CGO disabled)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/server ./cmd/server/main.go

# Stage 2: Build the minimal runner image
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy compiled binaries from builder stage
COPY --from=builder /app/server /app/server

# Document the port
EXPOSE 5001

# Set entrypoint to start the server
ENTRYPOINT ["/app/server"]
