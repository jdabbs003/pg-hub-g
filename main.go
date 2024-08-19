package pghub

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"time"
)

type Stats struct {
	Valid      bool
	MsgMax     uint32
	StartCount uint32
	QueueMax   uint32
	HubCount   uint32
}

type HubStats struct {
	Valid             bool
	RxEventCount      uint64
	SubscribeCount    uint32
	ListenCount       uint32
	ListenFailCount   uint32
	UnlistenCount     uint32
	UnlistenFailCount uint32
	NotifyCount       uint32
	NotifyFailCount   uint32
	ConnectCount      uint64
	ConnectFailCount  uint64
	Q1Max             uint32
	Q2Max             uint32
	ConsumerCount     uint32
}

type Config struct {
	Pool         *pgxpool.Pool
	Logger       *slog.Logger
	ConnectRetry time.Duration
}

type NotifyHook func(*Event)

type ConfirmHook func(bool)

func SetLogger(logger *slog.Logger) bool {
	return core.logger.CompareAndSwap(nil, logger)
}

func GetStats(stats *Stats) {
	exportStats(stats, &core.stats)
}

func New(name string, config *Config) (*Hub, error) {
	return core.makeHub(name, config)
}
