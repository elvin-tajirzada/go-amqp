![GitHub go.mod Go version (subdirectory of monorepo)](https://img.shields.io/github/go-mod/go-version/elvin-tacirzade/go-amqp?logo=go)
[![Go Reference](https://pkg.go.dev/badge/github.com/elvin-tacirzade/go-amqp.svg)](https://pkg.go.dev/github.com/elvin-tacirzade/go-amqp)

# Go RabbitMQ Consumer

This package uses the package provided by the [RabbitMQ core team](https://github.com/rabbitmq/amqp091-go).
## Goals
Provide auto reconnect
## Installation
```
go get -u github.com/elvin-tacirzade/go-amqp
```
## Usage
First we call a New() function. The function takes the following parameters:
1. `user` - Declare a RabbitMQ user.
2. `password` - Declare a RabbitMQ password
3. `host` - Declare a RabbitMQ host
4. `port` -Declare a RabbitMQ port
5. `reconnectTime` - Declare an auto reconnect time

New() function returns the RabbitMQ struct and error.

See the [example](https://github.com/elvin-tacirzade/go-amqp/tree/main/example) subdirectory for simple consumers executables.


