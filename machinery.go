package go_queue

import (
	"fmt"
	"github.com/RichardKnop/machinery/v2"
	redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
	redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/locks/eager"
	"log/slog"
	"sync"
)

type Machinery struct {
	log         *slog.Logger
	connections *Connections
}

func NewMachinery(connections *Connections, log *slog.Logger) *Machinery {
	return &Machinery{log: log, connections: connections}
}

func (m *Machinery) Producer(connection string) (*machinery.Server, error) {
	if m.connections == nil {
		return nil, fmt.Errorf("no connections found")
	}

	store := m.connections.producers()
	store.mu.Lock()
	defer store.mu.Unlock()

	if server := store.servers[connection]; server != nil {
		return server, nil
	}

	server, err := m.Server(connection, "default")
	if err != nil {
		return nil, err
	}
	if server != nil {
		store.servers[connection] = server
	}

	return server, nil
}

type producerStore struct {
	mu      sync.Mutex
	servers map[string]*machinery.Server
}

func (m *Machinery) Server(connection string, queue string) (*machinery.Server, error) {
	if m.connections == nil {
		return nil, fmt.Errorf("no connections found")
	}

	conn := m.connections.Get(connection)
	if conn == nil {
		return nil, fmt.Errorf("no connection %s found", connection)
	}

	if conn.Driver == DriverSync {
		return nil, nil
	}

	return m.redisServer(connection, queue), nil
}

func (m *Machinery) redisServer(connection string, queue string) *machinery.Server {
	if queue == "" {
		queue = "default"
	}

	conn := m.connections.Get(connection)

	cnf := &config.Config{
		DefaultQueue: queue,
		Redis:        &config.RedisConfig{DelayedTasksPollPeriod: 100},
	}

	dsn := ""
	if conn.Redis.Password == "" {
		dsn = fmt.Sprintf("%s:%s", conn.Redis.Host, conn.Redis.Port)
	} else {
		dsn = fmt.Sprintf("%s@%s:%s", conn.Redis.Password, conn.Redis.Host, conn.Redis.Port)
	}

	broker := redisbroker.NewGR(cnf, []string{dsn}, conn.Redis.Database)
	backend := redisbackend.NewGR(cnf, []string{dsn}, conn.Redis.Database)
	lock := eager.New()

	return machinery.NewServer(cnf, broker, backend, lock)
}
