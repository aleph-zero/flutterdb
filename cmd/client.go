package cmd

import (
	"github.com/aleph-zero/flutterdb/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Run an flutterdb client",
	Long:  "Run an flutterdb client",
	Run: func(cmd *cobra.Command, args []string) {
		config := client.NewConfig(
			client.WithRemoteAddr(viper.GetString("client.remote-addr")),
			client.WithRemotePort(viper.GetUint16("client.remote-port")))
		client.Bootstrap(config)
	},
}

const (
	remoteAddr = "127.0.0.1"
	remotePort = 1234
)

func init() {
	rootCmd.AddCommand(clientCmd)
	clientCmd.PersistentFlags().String("client.remote-addr", remoteAddr, "Address to connect to")
	clientCmd.PersistentFlags().Int("client.remote-port", remotePort, "Port to connect to")

	viper.BindPFlag("client.remote-addr", clientCmd.PersistentFlags().Lookup("client.remote-addr"))
	viper.BindPFlag("client.remote-port", clientCmd.PersistentFlags().Lookup("client.remote-port"))
}
