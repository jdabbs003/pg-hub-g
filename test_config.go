package pghub

func newTestConfig() *testConfig {
	config := &testConfig{}

	config.Host = "127.0.0.1"
	config.Port = 5432
	config.User = "test_user"
	config.Password = "test_pass"
	config.Name = "test_db"
	config.RootCa = nil
	config.ClientCert = nil
	config.ClientKey = nil

	return config
}
