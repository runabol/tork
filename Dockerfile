FROM golang:1.24-alpine

# Set destination for COPY
WORKDIR /app

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . ./

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o tork cmd/main.go

# Expose the port the app runs on
EXPOSE 8000

# Run the binary
CMD ["./tork", "run", "standalone"]
