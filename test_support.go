package pghub

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

type testConfig struct {
	Host       string
	Port       uint32
	User       string
	Password   string
	Name       string
	SslMode    string
	RootCa     []byte
	ClientCert []byte
	ClientKey  []byte
}

func initPgxPool(config *testConfig) (*pgxpool.Pool, error) {
	connectionString :=
		fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s connect_timeout=%d sslmode=%s",
			config.Host,
			config.Port,
			config.User,
			config.Password,
			config.Name,
			10,
			config.SslMode)

	connConfig, err1 := pgxpool.ParseConfig(connectionString)

	if err1 != nil {
		return nil, err1
	}

	var tlsConfig *tls.Config = nil

	if config.RootCa != nil {
		caCertPool := x509.NewCertPool()
		ok := caCertPool.AppendCertsFromPEM(config.RootCa)
		if !ok {
			return nil, errors.New("AppendCertsFromPEM failed")
		}

		tlsConfig = &tls.Config{
			ServerName: config.Host,
			RootCAs:    caCertPool,
		}
	}

	if (config.ClientCert != nil) && (config.ClientKey != nil) {
		keypair, err := tls.X509KeyPair(config.ClientCert, config.ClientKey)
		if err != nil {
			return nil, err
		}

		if tlsConfig == nil {
			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{keypair},
				ServerName:   config.Host,
			}
		} else {
			tlsConfig.Certificates = []tls.Certificate{keypair}
			tlsConfig.ServerName = config.Host
		}
	}

	if tlsConfig != nil {
		connConfig.ConnConfig.TLSConfig = tlsConfig
	}

	pool, err2 := pgxpool.NewWithConfig(context.Background(), connConfig)

	if err2 != nil {
		return nil, err2
	}

	if pool != nil {
		err3 := pool.Ping(context.Background())
		if err3 != nil {
			return nil, err3
		}
	}

	return pool, nil
}
