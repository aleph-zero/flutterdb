package cmd

import (
    "fmt"
    "os"
    "path/filepath"

    "github.com/spf13/cobra"
    "github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
    Use:   "andrewdb",
    Short: "A simple database",
    Long:  `AndrewDB: A simple database for the cloud`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
    err := rootCmd.Execute()
    if err != nil {
        os.Exit(1)
    }
}

func init() {
    cobra.OnInitialize(initConfig)

    // Here you will define your flags and configuration settings.
    // Cobra supports persistent flags, which, if defined here, will be global for your application.
    rootCmd.PersistentFlags().StringVar(
        &cfgFile, "config", "", "config file (default is $HOME/.config/andrewdb/andrewdb.yaml)")

    // Cobra also supports local flags, which will only run when this action is called directly.
    rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
    if cfgFile != "" {
        viper.SetConfigFile(cfgFile) // use config file from the flag.
    } else {
        home, err := os.UserHomeDir()
        cobra.CheckErr(err)

        viper.AddConfigPath(filepath.Join(home, ".config/andrewdb"))
        viper.SetConfigType("yaml")
        viper.SetConfigName("andrewdb")
    }

    viper.AutomaticEnv() // read in environment variables that match

    if err := viper.ReadInConfig(); err != nil {
        // it's ok if we don't have a config file, we can fall back to defaults
        if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    }
}
