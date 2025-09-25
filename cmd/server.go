package cmd

import (
    "fmt"
    "github.com/aleph-zero/flutterdb/server"
    "github.com/aleph-zero/flutterdb/service/metastore"
    "github.com/spf13/cobra"
    "github.com/spf13/viper"
    "os"
)

var serverCmd = &cobra.Command{
    Use:   "server",
    Short: "Run an flutterdb server",
    Long:  "Run an flutterdb server",
    Run: func(cmd *cobra.Command, args []string) {
        config := server.NewConfig(
            server.WithAddress(viper.GetString("server.addr")),
            server.WithPort(viper.GetUint16("server.port")),
            server.WithClusterConfig(server.NewClusterConfig(
                server.WithNodeName(viper.GetString("cluster.node-name")),
                server.WithMembershipListenAddr(viper.GetString("cluster.membership-listen-addr")),
                server.WithMembershipListenPort(viper.GetUint16("cluster.membership-listen-port")),
                server.WithMembershipJoinAddrs(viper.GetStringSlice("cluster.membership-join-addrs")))),
            server.WithMetastoreConfig(metastore.NewConfig(
                metastore.WithDirectory(viper.GetString("metastore.data-dir")))))
        server.Bootstrap(config)
    },
}

const (
    apiListenAddr        = "0.0.0.0"
    apiListenPort        = 1234
    membershipListenAddr = "127.0.0.1"
    membershipListenPort = 5678
    metastoreDataDir     = ".metastore"
)

func init() {
    rootCmd.AddCommand(serverCmd)

    hostname, err := os.Hostname()
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }

    // Cobra supports Persistent Flags which will work for this command and all subcommands, e.g.:
    serverCmd.PersistentFlags().String("server.addr", apiListenAddr, "Address to bind to")
    serverCmd.PersistentFlags().Uint16("server.port", apiListenPort, "Port to listen on")
    serverCmd.PersistentFlags().String("cluster.node-name", hostname, "Unique identifier for the server")
    serverCmd.PersistentFlags().String("cluster.membership-listen-addr", membershipListenAddr, "Cluster membership address")
    serverCmd.PersistentFlags().Uint16("cluster.membership-listen-port", membershipListenPort, "Cluster membership port")
    serverCmd.PersistentFlags().StringSlice("cluster.membership-join-addrs", nil, "Join existing cluster at these addresses")
    serverCmd.PersistentFlags().String("metastore.data-dir", metastoreDataDir, "Data directory for metastore")

    viper.BindPFlag("server.addr", serverCmd.PersistentFlags().Lookup("server.addr"))
    viper.BindPFlag("server.port", serverCmd.PersistentFlags().Lookup("server.port"))
    viper.BindPFlag("cluster.node-name", serverCmd.PersistentFlags().Lookup("cluster.node-name"))
    viper.BindPFlag("cluster.membership-listen-addr", serverCmd.PersistentFlags().Lookup("cluster.membership-listen-addr"))
    viper.BindPFlag("cluster.membership-listen-port", serverCmd.PersistentFlags().Lookup("cluster.membership-listen-port"))
    viper.BindPFlag("cluster.membership-join-addrs", serverCmd.PersistentFlags().Lookup("cluster.membership-join-addrs"))
    viper.BindPFlag("metastore.data-dir", serverCmd.PersistentFlags().Lookup("metastore.data-dir"))

    // Cobra supports local flags which will only run when this command is called directly, e.g.:
    // serverCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
