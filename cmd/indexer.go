package cmd

import (
	"github.com/aleph-zero/flutterdb/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var indexerCmd = &cobra.Command{
	Use:   "indexer",
	Short: "Run an indexer client",
	Long:  "Run an indexer client to bulk index documents from a file",
	Run: func(cmd *cobra.Command, args []string) {
		config := client.NewIndexerConfig(
			client.WithClientConfig(client.NewConfig(
				client.WithRemoteAddr(viper.GetString("client.remote-addr")),
				client.WithRemotePort(viper.GetUint16("client.remote-port")))),
			client.WithIndex(viper.GetString("client.indexer.index")),
			client.WithFilename(viper.GetString("client.indexer.file")))
		client.BootstrapIndexer(config)
	},
}

func init() {
	clientCmd.AddCommand(indexerCmd)
	indexerCmd.Flags().String("client.indexer.index", "", "Index name")
	indexerCmd.Flags().String("client.indexer.file", "", "File of documents to index")

	viper.BindPFlag("client.indexer.index", indexerCmd.Flags().Lookup("client.indexer.index"))
	viper.BindPFlag("client.indexer.file", indexerCmd.Flags().Lookup("client.indexer.file"))
}
