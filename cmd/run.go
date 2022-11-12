package cmd

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-redis/redis/v8"
	"github.com/lorenzoranucci/transactional-outbox-router/internal/app/run"
	kafka2 "github.com/lorenzoranucci/transactional-outbox-router/internal/pkg/kafka"
	redis2 "github.com/lorenzoranucci/transactional-outbox-router/internal/pkg/redis"
	"github.com/lorenzoranucci/transactional-outbox-router/pkg/kafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the application",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := canal.NewCanal(getCanalConfig())
		if err != nil {
			return err
		}

		ed, err := getKafkaEventDispatcher()
		if err != nil {
			return err
		}
		handler, err := run.NewEventHandler(
			getRedisStateHandler(),
			ed,
			viper.GetString("dbAggregateIDColumnName"),
			viper.GetString("dbPayloadColumnName"),
		)
		if err != nil {
			return err
		}

		runner := run.NewRunner(c, handler)

		return runner.Run()
	},
}

func init() {
	viper.MustBindEnv("dbHost", "DB_HOST")
	viper.MustBindEnv("dbPort", "DB_PORT")
	viper.MustBindEnv("dbUser", "DB_USER")
	viper.MustBindEnv("dbPassword", "DB_PASSWORD")
	viper.MustBindEnv("dbOutboxTableRef", "DB_OUTBOX_TABLE_REF")
	viper.MustBindEnv("dbAggregateIDColumnName", "DB_AGGREGATE_ID_COLUMN_NAME")
	viper.MustBindEnv("dbPayloadColumnName", "DB_PAYLOAD_COLUMN_NAME")

	viper.MustBindEnv("kafkaHost", "KAFKA_HOST")
	viper.MustBindEnv("kafkaPort", "KAFKA_PORT")
	viper.MustBindEnv("kafkaTopic", "KAFKA_TOPIC")

	viper.MustBindEnv("redisHost", "REDIS_HOST")
	viper.MustBindEnv("redisPort", "REDIS_PORT")
	viper.MustBindEnv("redisDB", "REDIS_DB")
	viper.MustBindEnv("redisKey", "REDIS_KEY")

	rootCmd.AddCommand(runCmd)
}

func getKafkaEventDispatcher() (*kafka2.EventDispatcher, error) {
	producer, err := kafka.NewProducer(
		[]string{fmt.Sprintf("%s:%s", viper.GetString("kafkaHost"), viper.GetString("kafkaPort"))},
		viper.GetString("kafkaTopic"),
	)
	if err != nil {
		return nil, err
	}

	return kafka2.NewEventDispatcher(producer), nil
}

func getRedisStateHandler() *redis2.StateHandler {
	return redis2.NewStateHandler(
		redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%s", viper.GetString("redisHost"), viper.GetString("redisPort")),
			DB:   viper.GetInt("redisDB"),
		}),
		viper.GetString("redisKey"),
	)
}

func getCanalConfig() *canal.Config {
	cfg := canal.NewDefaultConfig()

	cfg.Addr = fmt.Sprintf("%s:%s", viper.GetString("dbHost"), viper.GetString("dbPort"))
	cfg.User = viper.GetString("dbUser")
	cfg.Password = viper.GetString("dbPassword")
	cfg.Dump.ExecutionPath = ""
	cfg.IncludeTableRegex = []string{fmt.Sprintf("^%s$", viper.Get("dbOutboxTableRef"))}

	return cfg
}
