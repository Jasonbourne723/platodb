package commands

import (
	"fmt"
	"log"
	"net"

	"github.com/spf13/cobra"
)

var rootCommand = &cobra.Command{
	Use:   "plato-cli",
	Short: "A key-value database based on lsm-tree",
	Long:  "A key-value database based on lsm-tree",
	Run: func(cmd *cobra.Command, args []string) {

		server, _ := cmd.Flags().GetString("server")
		conn, err := ConnectToServer(server)
		if err != nil {
			fmt.Println("连接错误:", err)
			return
		}
		defer func(conn net.Conn) {
			err := conn.Close()
			if err != nil {
				log.Println(fmt.Errorf("connection close failed %w", err))
			}
		}(conn)
		HandleCommandLoop(conn)
	},
}

// Execute sets up the root command's persistent flags, specifically the server address and port, and then attempts to execute the root command. If an error occurs during execution, the function logs the error and exits the program. This function is typically used as the entry point for a CLI application.
func Execute() {

	rootCommand.PersistentFlags().String("server", "127.0.0.1:6399", "服务器地址和端口")
	if err := rootCommand.Execute(); err != nil {
		log.Fatal(err)
	}
}
