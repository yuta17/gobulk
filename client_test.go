package gobulk

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/ory/dockertest"
	"github.com/stretchr/testify/assert"
)

var testInputDB *sql.DB
var testOutputDB *sql.DB
var inputPort string
var outputPort string

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// for input
	// pulls an image, creates a container based on it and runs it
	inputResource, err := pool.Run("mysql", "5.7", []string{"MYSQL_ROOT_PASSWORD=input"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		inputPort = inputResource.GetPort("3306/tcp")
		testInputDB, err = sql.Open("mysql", fmt.Sprintf("root:input@(localhost:%s)/mysql", inputPort))
		if err != nil {
			return err
		}
		return testInputDB.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// for output
	outputResource, err := pool.Run("mysql", "5.7", []string{"MYSQL_ROOT_PASSWORD=output"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	if err := pool.Retry(func() error {
		var err error
		outputPort = outputResource.GetPort("3306/tcp")
		testOutputDB, err = sql.Open("mysql", fmt.Sprintf("root:output@(localhost:%s)/mysql", outputPort))
		if err != nil {
			return err
		}
		return testOutputDB.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(inputResource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	if err := pool.Purge(outputResource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestNewClient(t *testing.T) {
	inputURL := fmt.Sprintf("root:input@(localhost:%s)/mysql", inputPort)
	outputURL := fmt.Sprintf("root:output@(localhost:%s)/mysql", outputPort)
	client, _ := NewClient("mysql", "mysql", inputURL, outputURL)
	assert.NotNil(t, client.InputDB)
	assert.NotNil(t, client.OutputDB)
}

func TestSync(t *testing.T) {
	t.SkipNow()
}
